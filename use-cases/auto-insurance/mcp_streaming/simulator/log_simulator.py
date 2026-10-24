"""
Simulates an auto insurer's claims & telematics platform producing events to Kafka.

Healthy traffic (trips ingested, FNOLs created, coverage verified, fraud scored,
payouts issued ...) with a periodic incident every ~45s. Each incident replays one
coherent scenario from mcp_streaming.domain.auto_insurance — a telematics outage,
policy-DB exhaustion, fraud-model outage, vendor certificate expiry, payout
rejection, hailstorm (CAT) surge or a staged-accident fraud ring.

  python -m mcp_streaming.simulator.log_simulator                     # rotate built-in scenarios
  python -m mcp_streaming.simulator.log_simulator --scenario catastrophe_fnol_surge
  python -m mcp_streaming.simulator.log_simulator --ai                # an LLM invents each incident
  python -m mcp_streaming.simulator.log_simulator --list-scenarios

All data is synthetic.
"""

import argparse
import asyncio
import itertools
import json
import os
import random
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Iterator, List, Optional

from mcp_streaming.domain import auto_insurance as domain

# USE_MOCK_KAFKA=1 swaps in the in-process mock (used by mcp_streaming.demo; no Docker needed)
if os.environ.get("USE_MOCK_KAFKA") == "1":
    from mcp_streaming.messaging.mock_kafka import Producer
else:
    from confluent_kafka import Producer

SCENARIO_SYSTEM = ("You generate realistic, fully synthetic event streams for an auto-insurance "
                   "claims and telematics platform, for an observability demo.")
SCENARIO_PROMPT = """Invent ONE incident on an auto insurer's claims & telematics platform.

Services: {services}

Pick a plausible incident and vary it from run to run. Mix of technical faults (bad deploy,
database exhaustion, model endpoint outage, third-party estimating/parts/rental vendor failure,
expired certificate, telematics pipeline lag, message-queue backlog) and business anomalies
(catastrophe/weather FNOL surge, fraud ring, unusual claim severity, subrogation backlog).
Avoid repeating this recent one: {avoid}

Return ONLY a JSON array of 10-14 events, ordered as the incident unfolds: the root-cause
service fails first, then downstream services show symptoms. Each element:
{{"level": "WARN|ERROR|FATAL", "service": "<one of the services>",
  "event_type": "telematics|fnol|policy|claim|fraud|rating|payment",
  "message": "<realistic log line, <140 chars>"}}

Use only synthetic identifiers like POL-123456, CLM-654321, VEH-4821, RS-512.
Never include names, phone numbers, emails, street addresses or VINs."""

_producer = None


def _get_producer():
    global _producer
    if _producer is None:  # created on first use so --list-scenarios never touches Kafka
        _producer = Producer({"bootstrap.servers": "localhost:9092"})
    return _producer


def emit(event: dict):
    _get_producer().produce(domain.TOPIC, json.dumps(event).encode())


class AIScenarioGenerator:
    """Pre-generates LLM incident scenarios in a background thread so the 1s tick never blocks."""

    def __init__(self, provider: Optional[str] = None):
        from mcp_streaming.llm import create_llm  # lazy: the plain simulator needs no LLM deps
        self.llm = create_llm(provider)
        self.pool = ThreadPoolExecutor(max_workers=1)
        self.pending: Optional[Future] = None
        self.last_summary = "none"
        print(f"🧠 AI incident scenarios via {self.llm.describe()}")

    def prefetch(self):
        if self.pending is None:
            self.pending = self.pool.submit(self._generate)

    def take(self) -> List[dict]:
        """Return the prefetched scenario, or [] if not ready / failed (caller falls back)."""
        fut, self.pending = self.pending, None
        if fut is None or not fut.done():
            if fut is not None:
                self.pending = fut  # keep it for the next incident
            return []
        try:
            return fut.result()
        except Exception as exc:
            print(f"⚠️  AI scenario failed, using a built-in scenario: {exc}")
            return []

    def _generate(self) -> List[dict]:
        from mcp_streaming.llm import extract_json

        async def _ask():
            conv = self.llm.new_conversation(SCENARIO_SYSTEM)
            prompt = SCENARIO_PROMPT.format(services=", ".join(domain.SERVICES), avoid=self.last_summary)
            return await conv.ask(prompt, _no_tools)

        data = extract_json(asyncio.run(_ask()))
        region = random.choice(domain.REGIONS)
        events = []
        for item in data if isinstance(data, list) else []:
            if not isinstance(item, dict) or not item.get("message"):
                continue
            level = str(item.get("level", "ERROR")).upper()
            service = item.get("service") if item.get("service") in domain.SERVICES else random.choice(list(domain.SERVICES))
            # LLM output is untrusted: redact anything PII-shaped and don't let braces reach str.format
            message = domain.redact(str(item["message"])[:200]).replace("{", "(").replace("}", ")")
            events.append(domain.make_event(
                level if level in ("WARN", "ERROR", "FATAL") else "ERROR",
                service, message, str(item.get("event_type") or "claim")[:20], domain._ids(region)))
        if events:
            self.last_summary = events[0]["message"][:100]
            print(f"🧠 New AI incident ready: {self.last_summary}")
        return events


async def _no_tools(name, args):
    return {"error": "no tools available"}


def _incident_stream(scenario_key: Optional[str], rotation: Iterator[dict],
                     ai_events: List[dict]) -> Iterator[dict]:
    """Endless stream of events for one incident (same region; fresh IDs on each pass)."""
    if ai_events:
        while True:
            for e in ai_events:
                yield {**e, "request_id": f"req-{random.randint(10000, 99999)}"}
    scenario = domain.SCENARIOS_BY_KEY[scenario_key] if scenario_key else next(rotation)
    region = random.choice(domain.REGIONS)
    print(f"💥 Incident: {scenario['title']} ({region})")
    while True:
        yield from domain.render_scenario(scenario, region)


def run(ai: Optional[AIScenarioGenerator] = None, start_tick: int = 0, scenario: Optional[str] = None):
    """Produce forever. `start_tick` shifts the 45s cycle (incidents at ticks 30-40)."""
    print(f"Insurance event simulator → topic '{domain.TOPIC}'. Ctrl+C to stop.")
    rotation = itertools.cycle(random.sample(domain.SCENARIOS, len(domain.SCENARIOS)))
    tick = start_tick
    incident: Optional[Iterator[dict]] = None
    while True:
        # Healthy traffic
        for _ in range(random.randint(1, 3)):
            emit(domain.normal_event())
        if random.random() < 0.15:
            emit(domain.warn_event())

        tick += 1
        phase = tick % 45
        if ai and phase == 5:
            ai.prefetch()  # give the LLM ~25s to prepare the next incident
        if phase == 30:
            incident = _incident_stream(scenario, rotation, ai.take() if ai else [])
        if incident is not None and 30 <= phase <= 40:
            for _ in range(random.randint(3, 5)):
                emit(next(incident))
        elif phase == 41:
            incident = None

        _get_producer().flush()
        time.sleep(1)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Produce synthetic auto-insurance platform events to Kafka")
    parser.add_argument("--ai", action="store_true", help="let an LLM generate incident scenarios")
    parser.add_argument("--provider", default=None, help="bedrock | openai | ollama | mock")
    parser.add_argument("--scenario", choices=sorted(domain.SCENARIOS_BY_KEY), help="always replay this scenario")
    parser.add_argument("--list-scenarios", action="store_true", help="print the built-in scenarios and exit")
    args = parser.parse_args(argv)
    if args.list_scenarios:
        for s in domain.SCENARIOS:
            print(f"  {s['key']:<30} [{s['category']}] {s['title']}")
        return
    try:
        run(AIScenarioGenerator(args.provider) if args.ai else None, scenario=args.scenario)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
