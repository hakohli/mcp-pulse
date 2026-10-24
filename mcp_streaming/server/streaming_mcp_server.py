"""
Streaming MCP Server — pushes live log events to AI agents via WebSocket.
Consumes from Kafka topic 'app-logs', analyzes patterns, streams to subscribers.
"""

import asyncio
import json
import os
import time
from collections import deque
from typing import Dict, Optional, Set
import websockets

# USE_MOCK_KAFKA=1 swaps in the in-process mock (used by mcp_streaming.demo; no Docker needed)
if os.environ.get("USE_MOCK_KAFKA") == "1":
    from mcp_streaming.messaging.mock_kafka import Consumer
else:
    from confluent_kafka import Consumer

# --- Kafka-backed event stream ---

class LogStreamConsumer:
    """Consumes log events from Kafka and notifies subscribers."""

    def __init__(self, broker="localhost:9092", topic="app-logs"):
        self.topic = topic
        self.consumer = Consumer({
            "bootstrap.servers": broker,
            "group.id": "mcp-streaming-server",
            "auto.offset.reset": "latest",
        })
        self.consumer.subscribe([topic])
        self.subscribers: Set[asyncio.Queue] = set()
        self.recent: deque = deque(maxlen=100)  # rolling window for anomaly detection
        self.error_counts: Dict[str, int] = {}  # error pattern tracking

    def poll(self) -> Optional[dict]:
        msg = self.consumer.poll(0.1)
        if msg is None or msg.error():
            return None
        event = json.loads(msg.value().decode("utf-8"))
        self.recent.append(event)
        self._track_errors(event)
        return event

    def _track_errors(self, event: dict):
        if event.get("level") in ("ERROR", "FATAL"):
            key = event.get("message", "")[:80]
            self.error_counts[key] = self.error_counts.get(key, 0) + 1

    def get_anomaly_summary(self) -> dict:
        """Analyze recent window for anomalies."""
        if not self.recent:
            return {"status": "no_data"}
        errors = [e for e in self.recent if e.get("level") in ("ERROR", "FATAL")]
        error_rate = len(errors) / len(self.recent)
        top_errors = sorted(self.error_counts.items(), key=lambda x: -x[1])[:5]
        return {
            "window_size": len(self.recent),
            "error_rate": round(error_rate, 3),
            "spike": error_rate > 0.3,
            "top_errors": [{"message": m, "count": c} for m, c in top_errors],
            "total_errors": len(errors),
        }

    async def notify_subscribers(self, event: dict):
        dead = set()
        for q in self.subscribers:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                dead.add(q)
        self.subscribers -= dead

    def subscribe(self) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=500)
        self.subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        self.subscribers.discard(q)


# --- Streaming MCP Server over WebSocket ---

class StreamingMCPServer:
    """
    MCP server that supports both standard request/response AND streaming subscriptions.
    Protocol:
      -> {"method": "subscribe", "params": {"topic": "app-logs"}}
      <- {"type": "event", "data": {...}}  (continuous stream)
      -> {"method": "get_anomalies"}
      <- {"type": "response", "data": {...}}
      -> {"method": "get_context"}
      <- {"type": "response", "data": {...}}  (recent log window)
    """

    def __init__(self, log_stream: LogStreamConsumer):
        self.log_stream = log_stream

    async def handle_client(self, ws):
        sub_queue = None
        stream_task = None
        try:
            async for raw in ws:
                msg = json.loads(raw)
                method = msg.get("method")

                if method == "subscribe":
                    sub_queue = self.log_stream.subscribe()
                    stream_task = asyncio.create_task(
                        self._stream_events(ws, sub_queue)
                    )
                    await ws.send(json.dumps({
                        "type": "subscribed",
                        "topic": self.log_stream.topic,
                    }))

                elif method == "unsubscribe":
                    if sub_queue:
                        self.log_stream.unsubscribe(sub_queue)
                        if stream_task:
                            stream_task.cancel()
                    await ws.send(json.dumps({"type": "unsubscribed"}))

                elif method == "get_anomalies":
                    summary = self.log_stream.get_anomaly_summary()
                    await ws.send(json.dumps({"type": "response", "method": "get_anomalies", "data": summary}))

                elif method == "get_context":
                    # Standard MCP-style: return recent logs as snapshot
                    await ws.send(json.dumps({
                        "type": "response",
                        "method": "get_context",
                        "data": list(self.log_stream.recent),
                    }))

        finally:
            if sub_queue:
                self.log_stream.unsubscribe(sub_queue)
            if stream_task:
                stream_task.cancel()

    async def _stream_events(self, ws, queue: asyncio.Queue):
        try:
            while True:
                event = await queue.get()
                await ws.send(json.dumps({"type": "event", "data": event}))
        except (asyncio.CancelledError, websockets.ConnectionClosed):
            pass


# --- Kafka poll loop ---

async def kafka_poll_loop(log_stream: LogStreamConsumer):
    loop = asyncio.get_event_loop()
    while True:
        event = await loop.run_in_executor(None, log_stream.poll)
        if event:
            await log_stream.notify_subscribers(event)
        else:
            await asyncio.sleep(0.05)


# --- Main ---

async def main():
    log_stream = LogStreamConsumer()
    server = StreamingMCPServer(log_stream)

    # Loopback by default: this server has no authentication. Set MCP_HOST only
    # behind a gateway that authenticates callers.
    host = os.environ.get("MCP_HOST", "127.0.0.1")
    port = int(os.environ.get("MCP_PORT", "8765"))
    ws_server = await websockets.serve(server.handle_client, host, port)
    print(f"Streaming MCP Server running on ws://{host}:{port}")

    await asyncio.gather(
        kafka_poll_loop(log_stream),
        ws_server.wait_closed(),
    )

if __name__ == "__main__":
    asyncio.run(main())
