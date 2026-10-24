"""
Auto-insurance use-case tests: domain model, runbooks, PII redaction, Cedar roles,
and that the agent never hands PII to the LLM. Fully offline.

  python -m unittest tests.test_auto_insurance -v
"""

import asyncio
import json
import unittest

from mcp_streaming.agents.ai_agent import AIAgent, format_diagnosis
from mcp_streaming.domain import auto_insurance as domain
from mcp_streaming.llm import create_llm, extract_json
from mcp_streaming.server.cedar_authz import CedarAuthz


class ScenarioTest(unittest.TestCase):
    def test_every_scenario_renders_valid_events(self):
        for sc in domain.SCENARIOS:
            events = domain.render_scenario(sc, region="TX-DFW")
            self.assertEqual(len(events), len(sc["events"]), sc["key"])
            for e in events:
                self.assertIn(e["service"], domain.SERVICES, sc["key"])
                self.assertIn(e["level"], ("INFO", "WARN", "ERROR", "FATAL"))
                self.assertEqual(e["region"], "TX-DFW")
                self.assertNotIn("{", e["message"], "unfilled template placeholder")
            self.assertIn(sc["owner"], domain.OWNERS, sc["key"])
            self.assertTrue(sc["remediation"])

    def test_error_lines_map_back_to_their_own_runbook(self):
        for sc in domain.SCENARIOS:
            for e in domain.render_scenario(sc):
                if e["level"] == "ERROR":
                    match = domain.match_scenario(e["message"])
                    self.assertIsNotNone(match, e["message"])
                    self.assertEqual(match["key"], sc["key"], e["message"])

    def test_healthy_traffic_matches_no_runbook(self):
        for _ in range(300):
            e = domain.normal_event()
            self.assertEqual(e["level"], "INFO")
            self.assertIsNone(domain.match_scenario(e["message"]), e["message"])

    def test_search_runbooks(self):
        self.assertEqual(domain.search_runbooks("staged accident fraud ring repair shop")[0]["key"],
                         "staged_accident_ring")
        self.assertEqual(domain.search_runbooks("hail FNOL queue depth surge")[0]["key"],
                         "catastrophe_fnol_surge")
        self.assertEqual(domain.search_runbooks("zzz qqq"), [])

    def test_suggest_fix(self):
        self.assertIn("policy-db", domain.suggest_fix("DB connection pool exhausted (50/50) on policy-db-primary"))
        self.assertIn("Investigate", domain.suggest_fix("something unrelated"))


class RedactionTest(unittest.TestCase):
    def test_redacts_pii(self):
        text = ("Claimant jane.doe@example.com called from (555) 123-4567, SSN 123-45-6789, "
                "VIN 1HGCM82633A004352, card 4111 1111 1111 1111")
        out = domain.redact(text)
        for token in ("[EMAIL]", "[PHONE]", "[SSN]", "[VIN]", "[CARD]"):
            self.assertIn(token, out)
        for leaked in ("jane.doe", "123-4567", "123-45-6789", "1HGCM82633A004352", "4111"):
            self.assertNotIn(leaked, out)

    def test_does_not_mangle_operational_lines(self):
        for sc in domain.SCENARIOS:
            for e in domain.render_scenario(sc):
                self.assertEqual(domain.redact(e["message"]), e["message"], e["message"])
        for _ in range(200):
            e = domain.normal_event()
            self.assertEqual(domain.redact(e["message"]), e["message"], e["message"])


class CedarRolesTest(unittest.TestCase):
    def setUp(self):
        self.authz = CedarAuthz()

    def allowed(self, role, action, resource="insurance-events"):
        return self.authz.is_authorized({"id": "p", "role": role}, action, resource)

    def test_role_matrix(self):
        self.assertTrue(self.allowed("ai-agent", "subscribe"))
        self.assertTrue(self.allowed("ai-agent", "get_anomalies"))
        self.assertTrue(self.allowed("ai-agent", "get_context"))
        self.assertTrue(self.allowed("claims-ops", "get_anomalies"))
        self.assertFalse(self.allowed("claims-ops", "get_context"))
        self.assertTrue(self.allowed("siu-analyst", "get_context"))
        self.assertTrue(self.allowed("viewer", "subscribe"))
        self.assertFalse(self.allowed("viewer", "get_anomalies"))

    def test_ai_agent_forbidden_from_pii_stream(self):
        self.assertFalse(self.allowed("ai-agent", "subscribe", domain.PII_STREAM))
        self.assertIn("forbid", self.authz.explain({"id": "p", "role": "ai-agent"}, "subscribe", domain.PII_STREAM))


class FakeConn:
    """Stands in for MCPConnection: returns canned server responses."""

    def __init__(self, events):
        self.events = events

    async def request(self, method, params=None):
        if method == "get_context":
            return {"type": "response", "data": self.events}
        errors = [e for e in self.events if e["level"] in ("ERROR", "FATAL")]
        counts = {}
        for e in errors:
            counts[e["message"]] = counts.get(e["message"], 0) + 1
        top = sorted(counts.items(), key=lambda x: -x[1])
        return {"type": "response", "data": {
            "window_size": len(self.events), "error_rate": len(errors) / len(self.events),
            "spike": True, "total_errors": len(errors),
            "top_errors": [{"message": m, "count": c} for m, c in top]}}


class AgentPrivacyAndDiagnosisTest(unittest.TestCase):
    PII_EVENT = {"level": "ERROR", "service": "fnol-service", "region": "TX-DFW",
                 "message": "FNOL failed for claimant jane.doe@example.com, phone 555-123-4567"}

    def test_tools_redact_before_llm(self):
        agent = AIAgent(create_llm("mock"), show="none", out=lambda *_: None)
        agent.conn = FakeConn([self.PII_EVENT] * 3)
        ctx = asyncio.run(agent.execute_tool("get_context", {}))
        anomalies = asyncio.run(agent.execute_tool("get_anomalies", {}))
        blob = json.dumps([ctx, anomalies])
        self.assertNotIn("jane.doe", blob)
        self.assertNotIn("555-123-4567", blob)
        self.assertIn("[EMAIL]", blob)
        self.assertIn("[PHONE]", blob)

    def test_runbook_tool(self):
        agent = AIAgent(create_llm("mock"), show="none", out=lambda *_: None)
        out = asyncio.run(agent.execute_tool("search_runbooks", {"query": "payout batch rejected"}))
        self.assertEqual(out["runbooks"][0]["key"], "payout_batch_rejected")

    def test_mock_diagnoses_each_scenario_with_correct_owner(self):
        for sc in domain.SCENARIOS:
            incident = [e for e in domain.render_scenario(sc) * 3 if e["level"] == "ERROR"]
            healthy = [domain.normal_event() for _ in range(len(incident))]
            agent = AIAgent(create_llm("mock"), window=len(incident) + len(healthy), show="none",
                            out=lambda *_: None)
            for e in healthy + incident:
                agent.window.append(e)
            agent.conn = FakeConn(healthy + incident)
            d = asyncio.run(agent.diagnose())
            self.assertEqual(d["recommended_owner"], sc["owner"], sc["key"])
            self.assertEqual(d["category"], sc["category"], sc["key"])
            self.assertTrue(d["business_impact"] and d["remediation"], sc["key"])
            card = format_diagnosis(d)
            self.assertIn("Impact:", card)
            self.assertIn("humans", card.replace("people", "humans"))

    def test_chat_answer_is_prose(self):
        agent = AIAgent(create_llm("mock"), show="none", out=lambda *_: None)
        agent.conn = FakeConn([e for e in domain.render_scenario(domain.SCENARIOS_BY_KEY["catastrophe_fnol_surge"])])
        answer = asyncio.run(agent.ask("Is FNOL healthy?"))
        self.assertIsNone(extract_json(answer) if answer.strip().startswith("{") else None)
        self.assertIn("[mock]", answer)


if __name__ == "__main__":
    unittest.main()
