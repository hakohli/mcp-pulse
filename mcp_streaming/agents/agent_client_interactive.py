"""
Interactive AI Agent — subscribes to live stream + accepts user questions.
Uses a queue to route responses correctly between stream and user commands.
"""
import asyncio
import json
import sys
import websockets

ANOMALY_THRESHOLD = 0.3

FIX_SUGGESTIONS = {
    "Connection refused": "Check if the target service is running. Verify security groups and network ACLs.",
    "Timeout waiting": "Increase timeout or check downstream service health. Add circuit breaker.",
    "OutOfMemoryError": "Increase JVM heap: `-Xmx4g`. Check for memory leaks.",
    "SSL handshake failed": "Renew certificate: `certbot renew`.",
    "Deadlock detected": "Review transaction isolation levels. Consider optimistic locking.",
    "Disk usage critical": "Clean old logs. Add log rotation. Expand volume.",
}

def suggest_fix(message: str) -> str:
    for pattern, fix in FIX_SUGGESTIONS.items():
        if pattern in message:
            return fix
    return "Investigate service logs and recent deployments."

async def run_agent():
    uri = "ws://localhost:8765"
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"method": "subscribe", "params": {"topic": "app-logs"}}))
        ack = json.loads(await ws.recv())
        print(f"✅ {ack['type']} to {ack.get('topic', 'stream')}")
        print("💬 Commands: anomalies | context | status | help | quit\n")

        state = {"total": 0, "errors": []}
        response_queue = asyncio.Queue()

        async def reader():
            """Read all messages from WebSocket and route them."""
            async for raw in ws:
                msg = json.loads(raw)
                if msg["type"] == "event":
                    event = msg["data"]
                    state["total"] += 1
                    level = event.get("level", "INFO")
                    text = event.get("message", "")
                    ts = event.get("timestamp", "")
                    svc = event.get("service", "")

                    icon = {"INFO": "·", "WARN": "⚠", "ERROR": "❌", "FATAL": "💀"}.get(level, "·")
                    print(f"  {icon} [{ts}] {svc}: {text}")

                    if level in ("ERROR", "FATAL"):
                        state["errors"].append(event)
                        print(f"    🔧 {suggest_fix(text)}")

                    if state["total"] % 20 == 0 and state["errors"]:
                        rate = len(state["errors"]) / 20
                        if rate > ANOMALY_THRESHOLD:
                            print(f"\n  🚨 ANOMALY — error rate {rate:.0%} in last 20 events\n")
                        state["errors"].clear()
                else:
                    # Response to a user command
                    await response_queue.put(msg)

        async def input_handler():
            """Read user commands from stdin."""
            loop = asyncio.get_event_loop()
            while True:
                line = await loop.run_in_executor(None, sys.stdin.readline)
                if not line:
                    break
                cmd = line.strip().lower()
                if not cmd:
                    continue
                if cmd == "anomalies":
                    await ws.send(json.dumps({"method": "get_anomalies"}))
                    resp = await response_queue.get()
                    summary = resp.get("data", {})
                    print(f"\n📊 Anomaly Summary: {json.dumps(summary, indent=2)}\n")
                elif cmd == "context":
                    await ws.send(json.dumps({"method": "get_context"}))
                    resp = await response_queue.get()
                    print(f"\n📋 Context: {json.dumps(resp.get('data', {}), indent=2)}\n")
                elif cmd == "status":
                    print(f"\n📈 Events seen: {state['total']}, Errors in window: {len(state['errors'])}\n")
                elif cmd == "help":
                    print("\n  Commands: anomalies | context | status | help | quit\n")
                elif cmd == "quit":
                    return
                else:
                    print(f"\n  Unknown command: {cmd}. Type 'help' for options.\n")

        await asyncio.gather(reader(), input_handler())

if __name__ == "__main__":
    print("🤖 Interactive AI Agent — Live Log Monitor")
    print("=" * 50)
    asyncio.run(run_agent())
