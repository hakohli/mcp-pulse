"""
Simulates an application producing log events to Kafka.
Generates normal traffic with periodic error spikes to demo anomaly detection.

  python -m mcp_streaming.simulator.log_simulator          # static error messages
  python -m mcp_streaming.simulator.log_simulator --ai     # an LLM invents a fresh,
                                                           # causally-coherent incident per spike
"""

import argparse
import asyncio
import json
import random
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import List, Optional

import os

# USE_MOCK_KAFKA=1 swaps in the in-process mock (used by mcp_streaming.demo; no Docker needed)
if os.environ.get("USE_MOCK_KAFKA") == "1":
    from mcp_streaming.messaging.mock_kafka import Producer
else:
    from confluent_kafka import Producer

SERVICES = ["api-gateway", "auth-service", "payment-service", "order-service", "inventory-db"]
NORMAL_MESSAGES = [
    "Request processed successfully",
    "Cache hit for user session",
    "Database query completed in 12ms",
    "Health check passed",
    "Connection pool: 23/50 active",
]
ERROR_MESSAGES = [
    "Connection refused to inventory-db:5432",
    "Timeout waiting for payment-service response (30s)",
    "OutOfMemoryError: Java heap space",
    "SSL handshake failed: certificate expired",
    "Deadlock detected in transaction pool",
    "Disk usage critical: /var/log at 97%",
]

SCENARIO_SYSTEM = "You generate realistic synthetic application logs for an observability demo."
SCENARIO_PROMPT = """Invent ONE production incident for a system with these services: {services}.
Pick a plausible root cause (e.g. bad deploy, expired cert, DB lock contention, memory leak,
DNS failure, exhausted connection pool, noisy neighbour, misconfigured feature flag) — vary it.
Avoid this recent one: {avoid}

Return ONLY a JSON array of 12-16 log events, ordered as the incident unfolds: the root-cause
service fails first, then downstream services show symptoms. Each element:
{{"level": "WARN|ERROR|FATAL", "service": "<one of the services>", "message": "<realistic log line, <140 chars>"}}"""

producer = Producer({"bootstrap.servers": "localhost:9092"})


def make_log(level="INFO", spike=False):
    service = random.choice(SERVICES)
    if spike or level in ("ERROR", "FATAL"):
        msg = random.choice(ERROR_MESSAGES)
    else:
        msg = random.choice(NORMAL_MESSAGES)
    return {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "level": level,
        "service": service,
        "message": msg,
        "request_id": f"req-{random.randint(10000,99999)}",
    }


class AIScenarioGenerator:
    """Pre-generates LLM incident scenarios in a background thread so the 1s tick never blocks."""

    def __init__(self, provider: Optional[str] = None):
        from mcp_streaming.llm import create_llm  # imported lazily so the plain simulator needs no LLM deps
        self.llm = create_llm(provider)
        self.pool = ThreadPoolExecutor(max_workers=1)
        self.pending: Optional[Future] = None
        self.last_summary = "none"
        print(f"🧠 AI scenarios via {self.llm.describe()}")

    def prefetch(self):
        if self.pending is None:
            self.pending = self.pool.submit(self._generate)

    def take(self) -> List[dict]:
        """Return the prefetched scenario, or [] if not ready / failed (caller falls back)."""
        fut, self.pending = self.pending, None
        if fut is None or not fut.done():
            if fut is not None:
                self.pending = fut  # keep it for the next spike
            return []
        try:
            return fut.result()
        except Exception as exc:
            print(f"⚠️  AI scenario failed, using static errors: {exc}")
            return []

    def _generate(self) -> List[dict]:
        from mcp_streaming.llm import extract_json

        async def _ask():
            conv = self.llm.new_conversation(SCENARIO_SYSTEM)
            return await conv.ask(SCENARIO_PROMPT.format(services=", ".join(SERVICES),
                                                         avoid=self.last_summary), _no_tools)

        data = extract_json(asyncio.run(_ask()))
        events = []
        for item in data if isinstance(data, list) else []:
            if not isinstance(item, dict) or not item.get("message"):
                continue
            level = str(item.get("level", "ERROR")).upper()
            events.append({
                "level": level if level in ("WARN", "ERROR", "FATAL") else "ERROR",
                "service": item.get("service") if item.get("service") in SERVICES else random.choice(SERVICES),
                "message": str(item["message"])[:200],
            })
        if events:
            self.last_summary = events[0]["message"][:100]
            print(f"🧠 New AI incident ready: {self.last_summary}")
        return events


async def _no_tools(name, args):
    return {"error": "no tools available"}


def emit(event: dict):
    producer.produce("app-logs", json.dumps(event).encode())


def run(ai: Optional[AIScenarioGenerator] = None, start_tick: int = 0):
    """Produce forever. `start_tick` shifts the 45s cycle (spikes at ticks 30-40)."""
    print("Log simulator running. Ctrl+C to stop.")
    tick = start_tick
    scenario: List[dict] = []
    while True:
        # Normal traffic
        for _ in range(random.randint(1, 3)):
            emit(make_log("INFO"))

        # Occasional warnings
        if random.random() < 0.15:
            emit(make_log("WARN"))

        # Error spike every ~30 seconds for ~10 seconds
        tick += 1
        phase = tick % 45
        if ai and phase == 5:
            ai.prefetch()  # give the LLM ~25s to prepare the next incident
        if ai and phase == 30:
            scenario = ai.take()

        if 30 <= phase <= 40:
            if scenario:
                # Replay the AI incident in order, a few lines per tick
                for _ in range(min(len(scenario), random.randint(2, 4))):
                    item = scenario.pop(0)
                    emit({**make_log(item["level"]), "service": item["service"], "message": item["message"]})
                # Keep the error rate high for the rest of the spike window
                for _ in range(random.randint(1, 2)):
                    emit(make_log("ERROR", spike=True))
            else:
                for _ in range(random.randint(3, 6)):
                    emit(make_log("ERROR", spike=True))
                if random.random() < 0.3:
                    emit(make_log("FATAL", spike=True))
        elif phase == 41:
            scenario = []

        producer.flush()
        time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produce synthetic app logs to Kafka")
    parser.add_argument("--ai", action="store_true", help="let an LLM generate incident scenarios")
    parser.add_argument("--provider", default=None, help="bedrock | openai | ollama | mock")
    args = parser.parse_args()
    try:
        run(AIScenarioGenerator(args.provider) if args.ai else None)
    except KeyboardInterrupt:
        pass
