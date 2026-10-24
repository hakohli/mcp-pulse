"""
Streaming MCP Server with Cedar authorization.
Uses mock_kafka for local testing, real confluent_kafka in production.
"""

import asyncio
import json
import os
from collections import deque
from typing import Optional, Dict, Set

# Use mock Kafka for local testing, real Kafka in production
if os.environ.get("USE_REAL_KAFKA"):
    from confluent_kafka import Consumer, Producer
else:
    from mcp_streaming.messaging.mock_kafka import Consumer, Producer

import websockets
from mcp_streaming.server.cedar_authz import CedarAuthz


class LogStreamConsumer:
    def __init__(self, broker="localhost:9092", topic="app-logs"):
        self.topic = topic
        self.consumer = Consumer({
            "bootstrap.servers": broker,
            "group.id": "mcp-streaming-server",
            "auto.offset.reset": "latest",
        })
        self.consumer.subscribe([topic])
        self.subscribers = set()  # type: Set[asyncio.Queue]
        self.recent = deque(maxlen=100)
        self.error_counts = {}  # type: Dict[str, int]

    def poll(self):
        # type: () -> Optional[dict]
        msg = self.consumer.poll(0.1)
        if msg is None or (hasattr(msg, 'error') and callable(msg.error) and msg.error()):
            return None
        event = json.loads(msg.value().decode("utf-8"))
        self.recent.append(event)
        if event.get("level") in ("ERROR", "FATAL"):
            key = event.get("message", "")[:80]
            self.error_counts[key] = self.error_counts.get(key, 0) + 1
        return event

    def get_anomaly_summary(self):
        # type: () -> dict
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

    async def notify_subscribers(self, event):
        dead = set()
        for q in self.subscribers:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                dead.add(q)
        self.subscribers -= dead

    def subscribe(self):
        # type: () -> asyncio.Queue
        q = asyncio.Queue(maxsize=500)
        self.subscribers.add(q)
        return q

    def unsubscribe(self, q):
        self.subscribers.discard(q)


def verify_principal(msg):
    # type: (dict) -> Optional[dict]
    """
    Turn an `auth` message into a *verified* principal, or None to reject.

    FAIL-CLOSED STUB: no real verifier is wired, so every credential is rejected
    and the connection stays the anonymous "viewer" principal. Roles must come
    from a credential the server can verify (e.g. a JWT validated against your
    identity provider's JWKS, with the role taken from its claims) — never from
    what the client asserts about itself.

    MCP_AUTH_TEST_BYPASS=1 accepts the client-asserted principal. It exists only
    for local demos/tests against a loopback-bound server.
    """
    if os.environ.get("MCP_AUTH_TEST_BYPASS") != "1":
        return None
    p = msg.get("principal")
    if not isinstance(p, dict) or not isinstance(p.get("id"), str) or not isinstance(p.get("role"), str):
        return None
    return {"id": p["id"], "role": p["role"]}


class StreamingMCPServer:
    def __init__(self, log_stream):
        self.log_stream = log_stream
        self.authz = CedarAuthz()

    async def handle_client(self, ws, path=None):
        principal = {"id": "anonymous", "role": "viewer"}
        sub_queue = None
        stream_task = None
        try:
            async for raw in ws:
                msg = json.loads(raw)
                method = msg.get("method")

                if method == "auth":
                    verified = verify_principal(msg)
                    if verified is None:
                        await ws.send(json.dumps({
                            "type": "error", "code": "UNAUTHENTICATED",
                            "message": "credential could not be verified; no verifier is configured",
                        }))
                        continue
                    principal = verified
                    await ws.send(json.dumps({"type": "authenticated", "principal": principal}))
                    continue

                resource = msg.get("params", {}).get("topic", self.log_stream.topic)
                if not self.authz.is_authorized(principal, method, resource):
                    explanation = self.authz.explain(principal, method, resource)
                    await ws.send(json.dumps({
                        "type": "error", "code": "FORBIDDEN", "message": explanation,
                    }))
                    continue

                if method == "subscribe":
                    sub_queue = self.log_stream.subscribe()
                    stream_task = asyncio.create_task(self._stream_events(ws, sub_queue))
                    await ws.send(json.dumps({"type": "subscribed", "topic": self.log_stream.topic}))

                elif method == "unsubscribe":
                    if sub_queue:
                        self.log_stream.unsubscribe(sub_queue)
                        if stream_task:
                            stream_task.cancel()
                    await ws.send(json.dumps({"type": "unsubscribed"}))

                elif method == "get_anomalies":
                    await ws.send(json.dumps({
                        "type": "response", "method": "get_anomalies",
                        "data": self.log_stream.get_anomaly_summary(),
                    }))

                elif method == "get_context":
                    await ws.send(json.dumps({
                        "type": "response", "method": "get_context",
                        "data": list(self.log_stream.recent),
                    }))
        finally:
            if sub_queue:
                self.log_stream.unsubscribe(sub_queue)
            if stream_task:
                stream_task.cancel()

    async def _stream_events(self, ws, queue):
        try:
            while True:
                event = await queue.get()
                await ws.send(json.dumps({"type": "event", "data": event}))
        except (asyncio.CancelledError, websockets.ConnectionClosed):
            pass


async def kafka_poll_loop(log_stream):
    loop = asyncio.get_event_loop()
    while True:
        event = await loop.run_in_executor(None, log_stream.poll)
        if event:
            await log_stream.notify_subscribers(event)
        else:
            await asyncio.sleep(0.05)


async def main():
    log_stream = LogStreamConsumer()
    server = StreamingMCPServer(log_stream)
    host = os.environ.get("MCP_HOST", "127.0.0.1")  # loopback by default
    port = int(os.environ.get("MCP_PORT", "8765"))
    ws_server = await websockets.serve(server.handle_client, host, port)
    print(f"Streaming MCP Server (Cedar-authorized) on ws://{host}:{port}")
    await asyncio.gather(kafka_poll_loop(log_stream), ws_server.wait_closed())

if __name__ == "__main__":
    asyncio.run(main())
