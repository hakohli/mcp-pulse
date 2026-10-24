"""
Integration test: Cedar-authorized streaming MCP server + mock Kafka + WebSocket clients.
Runs everything in-process. No Docker needed.

  python -m unittest tests.test_integration -v
"""

import asyncio
import json
import random
import time
import os
import unittest

# Force mock kafka
os.environ.pop("USE_REAL_KAFKA", None)

from mcp_streaming.messaging.mock_kafka import Producer
from mcp_streaming.server.streaming_mcp_server_cedar import LogStreamConsumer, StreamingMCPServer, kafka_poll_loop
import websockets

SERVICES = ["api-gateway", "auth-service", "payment-service", "order-service", "inventory-db"]
ERROR_MESSAGES = [
    "Connection refused to inventory-db:5432",
    "Timeout waiting for payment-service response (30s)",
    "OutOfMemoryError: Java heap space",
    "SSL handshake failed: certificate expired",
    "Deadlock detected in transaction pool",
]
NORMAL_MESSAGES = [
    "Request processed successfully",
    "Cache hit for user session",
    "Health check passed",
]

producer = Producer({"bootstrap.servers": "localhost:9092"})


def produce_logs(count: int, error_burst: bool = False):
    for _ in range(count):
        if error_burst:
            level = random.choice(["ERROR", "FATAL"])
            msg = random.choice(ERROR_MESSAGES)
        else:
            level = "INFO"
            msg = random.choice(NORMAL_MESSAGES)
        event = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "level": level,
            "service": random.choice(SERVICES),
            "message": msg,
            "request_id": f"req-{random.randint(10000,99999)}",
        }
        producer.produce("app-logs", json.dumps(event).encode())


async def run_test():
    print("=" * 60)
    print("🧪 Integration Test: Streaming MCP with Cedar Authorization")
    print("=" * 60)

    # Start server
    log_stream = LogStreamConsumer()
    server = StreamingMCPServer(log_stream)
    ws_server = await websockets.serve(server.handle_client, "127.0.0.1", 0)
    uri = f"ws://127.0.0.1:{ws_server.sockets[0].getsockname()[1]}"
    poll_task = asyncio.create_task(kafka_poll_loop(log_stream))

    await asyncio.sleep(0.3)

    # --- Test 1: Cedar auth - ops-agent can subscribe and get_anomalies ---
    print("\n📋 Test 1: ops-agent subscribes to app-logs")
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"method": "auth", "principal": {"id": "agent-1", "role": "ops-agent"}}))
        resp = json.loads(await ws.recv())
        print(f"   Auth: {resp['type']}")
        assert resp["type"] == "authenticated", resp

        await ws.send(json.dumps({"method": "subscribe", "params": {"topic": "app-logs"}}))
        resp = json.loads(await ws.recv())
        print(f"   Subscribe: {resp['type']} ✅")
        assert resp["type"] == "subscribed", resp

        # Produce normal logs
        produce_logs(5)
        await asyncio.sleep(0.5)

        events = []
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=0.3)
                events.append(json.loads(raw))
            except asyncio.TimeoutError:
                break
        print(f"   Received {len(events)} events ✅")
        assert len(events) >= 5, events

        # Produce error burst
        print("\n📋 Test 2: Error burst + anomaly detection")
        produce_logs(15, error_burst=True)
        await asyncio.sleep(0.5)

        errors = []
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=0.3)
                msg = json.loads(raw)
                if msg["type"] == "event" and msg["data"]["level"] in ("ERROR", "FATAL"):
                    errors.append(msg)
            except asyncio.TimeoutError:
                break
        print(f"   Received {len(errors)} error events ✅")
        assert len(errors) >= 15, len(errors)

        # Get anomaly summary
        await ws.send(json.dumps({"method": "get_anomalies"}))
        resp = json.loads(await ws.recv())
        summary = resp["data"]
        assert summary["spike"] is True, summary
        print(f"   Anomaly summary: error_rate={summary['error_rate']}, spike={summary['spike']} ✅")
        if summary.get("top_errors"):
            for e in summary["top_errors"][:3]:
                print(f"     - ({e['count']}x) {e['message']}")

    # --- Test 3: Cedar auth - viewer DENIED get_anomalies ---
    print("\n📋 Test 3: viewer role denied get_anomalies (Cedar forbid)")
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"method": "auth", "principal": {"id": "agent-2", "role": "viewer"}}))
        await ws.recv()

        await ws.send(json.dumps({"method": "get_anomalies"}))
        resp = json.loads(await ws.recv())
        print(f"   Response: {resp['type']} - {resp.get('code', '')} ✅")
        assert (resp["type"], resp.get("code")) == ("error", "FORBIDDEN"), resp
        print(f"   Reason: {resp.get('message', '')}")

    # --- Test 4: Cedar auth - readonly DENIED subscribe to audit-logs ---
    print("\n📋 Test 4: readonly role denied subscribe to audit-logs (Cedar forbid)")
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"method": "auth", "principal": {"id": "agent-3", "role": "readonly"}}))
        await ws.recv()

        await ws.send(json.dumps({"method": "subscribe", "params": {"topic": "audit-logs"}}))
        resp = json.loads(await ws.recv())
        print(f"   Response: {resp['type']} - {resp.get('code', '')} ✅")
        assert (resp["type"], resp.get("code")) == ("error", "FORBIDDEN"), resp
        print(f"   Reason: {resp.get('message', '')}")

    # --- Test 5: Cedar auth - senior-ops CAN get_context ---
    print("\n📋 Test 5: senior-ops gets full context window")
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"method": "auth", "principal": {"id": "agent-4", "role": "senior-ops"}}))
        await ws.recv()

        await ws.send(json.dumps({"method": "get_context"}))
        resp = json.loads(await ws.recv())
        assert resp["type"] == "response", resp
        ctx = resp["data"]
        assert len(ctx) > 0
        print(f"   Context window: {len(ctx)} events ✅")

    print("\n" + "=" * 60)
    print("✅ All tests passed!")
    print("=" * 60)

    poll_task.cancel()
    ws_server.close()
    await ws_server.wait_closed()


class CedarIntegrationTest(unittest.TestCase):
    def test_cedar_authorized_streaming(self):
        # Tests present self-asserted roles; allowed only via this explicit opt-in.
        os.environ["MCP_AUTH_TEST_BYPASS"] = "1"
        try:
            asyncio.run(run_test())
        finally:
            os.environ.pop("MCP_AUTH_TEST_BYPASS", None)


if __name__ == "__main__":
    unittest.main()
