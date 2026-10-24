"""
AI Agent client that subscribes to the Streaming MCP Server,
receives live log events, detects anomalies, and suggests fixes.
"""

import asyncio
import json
import websockets

from mcp_streaming.domain import auto_insurance as domain

ANOMALY_THRESHOLD = 0.3

def suggest_fix(message: str) -> str:
    """First step of the matching auto-insurance runbook."""
    return domain.suggest_fix(message)

async def run_agent():
    uri = "ws://localhost:8765"
    async with websockets.connect(uri) as ws:
        # Subscribe to live stream
        await ws.send(json.dumps({"method": "subscribe", "params": {"topic": domain.TOPIC}}))
        ack = json.loads(await ws.recv())
        print(f"✅ {ack['type']} to {ack.get('topic', 'stream')}\n")

        error_window = []
        total = 0

        async for raw in ws:
            msg = json.loads(raw)
            if msg["type"] != "event":
                continue

            event = msg["data"]
            total += 1
            level = event.get("level", "INFO")
            ts = event.get("timestamp", "")
            svc = event.get("service", "")
            text = event.get("message", "")

            # Print all events (compact)
            icon = {"INFO": "·", "WARN": "⚠", "ERROR": "❌", "FATAL": "💀"}.get(level, "·")
            print(f"  {icon} [{ts}] {svc}: {text}")

            # Track errors in sliding window
            if level in ("ERROR", "FATAL"):
                error_window.append(event)
                fix = suggest_fix(text)
                print(f"    🔧 Suggested fix: {fix}")

            # Check for anomaly spike every 20 events
            if total % 20 == 0 and error_window:
                rate = len(error_window) / 20
                if rate > ANOMALY_THRESHOLD:
                    print(f"\n  🚨 ANOMALY DETECTED — error rate {rate:.0%} in last 20 events")
                    # Ask server for full anomaly summary
                    await ws.send(json.dumps({"method": "get_anomalies"}))
                    resp = json.loads(await ws.recv())
                    summary = resp.get("data", {})
                    if summary.get("top_errors"):
                        print(f"  📊 Top errors:")
                        for e in summary["top_errors"][:3]:
                            print(f"     - ({e['count']}x) {e['message']}")
                    print()
                error_window.clear()

if __name__ == "__main__":
    print("🤖 Rule-based Agent (no LLM) — Auto Insurance Event Monitor")
    print("=" * 50)
    asyncio.run(run_agent())
