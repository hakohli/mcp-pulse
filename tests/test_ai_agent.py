"""
Tests for the LLM layer and the AI agent. Fully offline:
mock Kafka + mock LLM provider + in-process Cedar server on loopback.

  python -m unittest tests.test_ai_agent -v
"""

import asyncio
import json
import os
import unittest

os.environ.pop("USE_REAL_KAFKA", None)

import websockets

from mcp_streaming.agents.ai_agent import AIAgent, MCPConnection, filter_events, format_diagnosis
from mcp_streaming.llm import LLMError, Tool, create_llm, extract_json
from mcp_streaming.messaging.mock_kafka import Producer
from mcp_streaming.server.streaming_mcp_server_cedar import LogStreamConsumer, StreamingMCPServer, kafka_poll_loop, verify_principal

producer = Producer({"bootstrap.servers": "unused"})


def produce(level, service, message, n=1):
    for _ in range(n):
        producer.produce("app-logs", json.dumps({
            "timestamp": "2024-01-01T00:00:00", "level": level,
            "service": service, "message": message, "request_id": "req-1",
        }).encode())


class ExtractJsonTest(unittest.TestCase):
    def test_plain_fenced_and_embedded(self):
        self.assertEqual(extract_json('{"a": 1}'), {"a": 1})
        self.assertEqual(extract_json('Here:\n```json\n{"a": 2}\n```'), {"a": 2})
        self.assertEqual(extract_json('Sure! {"a": 3} hope that helps'), {"a": 3})
        self.assertEqual(extract_json('[{"x": 1}]'), [{"x": 1}])
        self.assertIsNone(extract_json("no json here"))


class FactoryTest(unittest.TestCase):
    def test_unknown_provider(self):
        with self.assertRaises(LLMError):
            create_llm("nope")

    def test_openai_without_key_fails_at_startup(self):
        saved = os.environ.pop("OPENAI_API_KEY", None)
        saved_url = os.environ.pop("OPENAI_BASE_URL", None)
        try:
            with self.assertRaises(LLMError) as ctx:
                create_llm("openai")
            self.assertIn("OPENAI_API_KEY is not set", str(ctx.exception))
            # Local OpenAI-compatible servers don't need a key
            self.assertEqual(create_llm("ollama").name, "openai")
        finally:
            if saved is not None:
                os.environ["OPENAI_API_KEY"] = saved
            if saved_url is not None:
                os.environ["OPENAI_BASE_URL"] = saved_url

    def test_http_error_message_is_trimmed(self):
        import io
        import urllib.error
        from unittest import mock
        from mcp_streaming.llm import OpenAICompatLLM
        llm = OpenAICompatLLM(model="m", base_url="http://127.0.0.1:1/v1", api_key="k")
        body = json.dumps({"error": {"message": "Incorrect API key provided.", "type": "invalid_request_error"}})
        err = urllib.error.HTTPError("u", 401, "Unauthorized", {}, io.BytesIO(body.encode()))
        with mock.patch("urllib.request.urlopen", side_effect=err):
            with self.assertRaises(LLMError) as ctx:
                llm._post({})
        self.assertEqual(str(ctx.exception), "HTTP 401 from http://127.0.0.1:1/v1: Incorrect API key provided.")

    def test_mock_provider(self):
        self.assertEqual(create_llm("mock").describe(), "mock")


class FilterEventsTest(unittest.TestCase):
    EVENTS = [
        {"level": "INFO", "service": "a", "message": "1"},
        {"level": "ERROR", "service": "a", "message": "2"},
        {"level": "FATAL", "service": "b", "message": "3"},
    ]

    def test_filters(self):
        self.assertEqual(filter_events(self.EVENTS, {"level": "ERROR_OR_FATAL"})["count"], 2)
        self.assertEqual(filter_events(self.EVENTS, {"service": "b"})["events"][0]["message"], "3")
        self.assertEqual(filter_events(self.EVENTS, {"limit": 1})["events"][0]["message"], "3")  # newest first
        self.assertEqual(filter_events(self.EVENTS, {"limit": "bad"})["count"], 3)


class ToolLoopErrorHandlingTest(unittest.TestCase):
    def test_tool_exception_is_reported_not_raised(self):
        async def boom(name, args):
            raise RuntimeError("server down")

        conv = create_llm("mock").new_conversation("sys", [Tool("get_anomalies", "x")])
        out = asyncio.run(conv.ask("diagnose; reply with ONLY a JSON object", boom))
        self.assertEqual(extract_json(out)["severity"], "low")  # mock saw {"error": ...}, no top_errors


class AuthStubTest(unittest.TestCase):
    def test_fail_closed_without_bypass(self):
        os.environ.pop("MCP_AUTH_TEST_BYPASS", None)
        self.assertIsNone(verify_principal({"principal": {"id": "x", "role": "senior-ops"}}))

    def test_bypass_validates_shape(self):
        os.environ["MCP_AUTH_TEST_BYPASS"] = "1"
        try:
            self.assertEqual(verify_principal({"principal": {"id": "x", "role": "ops-agent"}}),
                             {"id": "x", "role": "ops-agent"})
            self.assertIsNone(verify_principal({"principal": "admin"}))
        finally:
            os.environ.pop("MCP_AUTH_TEST_BYPASS", None)


class EndToEndTest(unittest.TestCase):
    """Spike on the stream -> agent detects -> mock LLM calls MCP tools -> diagnosis."""

    def test_spike_triggers_diagnosis(self):
        asyncio.run(self._run())

    async def _run(self):
        os.environ["MCP_AUTH_TEST_BYPASS"] = "1"
        log_stream = LogStreamConsumer(topic="app-logs")
        server = StreamingMCPServer(log_stream)
        ws_server = await websockets.serve(server.handle_client, "127.0.0.1", 0)
        port = ws_server.sockets[0].getsockname()[1]
        poll = asyncio.ensure_future(kafka_poll_loop(log_stream))
        lines = []
        try:
            agent = AIAgent(create_llm("mock"), window=10, threshold=0.3, cooldown=0, show="none",
                            out=lines.append)
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await ws.send(json.dumps({"method": "auth", "principal": {"id": "t", "role": "ops-agent"}}))
                self.assertEqual(json.loads(await ws.recv())["type"], "authenticated")
                await ws.send(json.dumps({"method": "subscribe", "params": {"topic": "app-logs"}}))
                self.assertEqual(json.loads(await ws.recv())["type"], "subscribed")

                conn = MCPConnection(ws, agent.on_event)
                agent.conn = conn
                reader = asyncio.ensure_future(conn.reader())

                produce("INFO", "api-gateway", "Request processed successfully", 4)
                produce("ERROR", "inventory-db", "Connection refused to inventory-db:5432", 8)

                for _ in range(50):
                    if agent.diagnoses and not agent.diagnosing:
                        break
                    await asyncio.sleep(0.1)
                reader.cancel()

            self.assertTrue(agent.diagnoses, "agent never produced a diagnosis")
            d = agent.diagnoses[0]
            self.assertIn(d["severity"], ("high", "critical"))
            self.assertIn("Connection refused", d["root_cause"])
            self.assertTrue(d["remediation"])
            self.assertIn("AI DIAGNOSIS", "\n".join(lines))
        finally:
            poll.cancel()
            ws_server.close()
            await ws_server.wait_closed()
            os.environ.pop("MCP_AUTH_TEST_BYPASS", None)

    def test_forbidden_tool_is_reported(self):
        asyncio.run(self._forbidden())

    async def _forbidden(self):
        os.environ["MCP_AUTH_TEST_BYPASS"] = "1"
        log_stream = LogStreamConsumer(topic="app-logs")
        ws_server = await websockets.serve(StreamingMCPServer(log_stream).handle_client, "127.0.0.1", 0)
        port = ws_server.sockets[0].getsockname()[1]
        try:
            agent = AIAgent(create_llm("mock"), show="none", out=lambda *_: None)
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await ws.send(json.dumps({"method": "auth", "principal": {"id": "t", "role": "ops-agent"}}))
                await ws.recv()
                agent.conn = MCPConnection(ws, agent.on_event)
                reader = asyncio.ensure_future(agent.conn.reader())
                result = await agent.execute_tool("get_context", {})
                reader.cancel()
            self.assertEqual(result["error"], "FORBIDDEN")
        finally:
            ws_server.close()
            await ws_server.wait_closed()
            os.environ.pop("MCP_AUTH_TEST_BYPASS", None)


class DetectorTest(unittest.TestCase):
    def test_first_spike_triggers_with_default_cooldown(self):
        # Regression: monotonic() can be near 0 at startup; first spike must not be suppressed.
        agent = AIAgent(create_llm("mock"), window=10, show="none", out=lambda *_: None)
        for _ in range(6):
            agent.window.append({"level": "INFO"})
        for _ in range(4):
            agent.window.append({"level": "ERROR"})
        self.assertTrue(agent.should_diagnose())

    def test_cooldown_and_threshold(self):
        agent = AIAgent(create_llm("mock"), window=10, cooldown=30, show="none", out=lambda *_: None)
        agent.window.extend([{"level": "INFO"}] * 10)
        self.assertFalse(agent.should_diagnose())  # below threshold
        agent.window.extend([{"level": "ERROR"}] * 5)
        import time
        agent.last_diagnosis_at = time.monotonic()
        self.assertFalse(agent.should_diagnose())  # in cooldown


class ProviderToolLoopTest(unittest.TestCase):
    """Exercise the real Bedrock/OpenAI message plumbing with fake transports (no network)."""

    @staticmethod
    async def _tool(name, args):
        return {"error_rate": 0.5, "seen": name}

    def test_bedrock_tool_round_trip(self):
        from mcp_streaming.llm import BedrockLLM
        calls = []

        class FakeClient:
            def converse(self, **kw):
                calls.append(kw)
                if len(calls) == 1:
                    return {"stopReason": "tool_use", "output": {"message": {"role": "assistant", "content": [
                        {"toolUse": {"toolUseId": "t1", "name": "get_anomalies", "input": {}}}]}}}
                return {"stopReason": "end_turn", "output": {"message": {"role": "assistant",
                                                                          "content": [{"text": '{"severity":"high"}'}]}}}

        llm = BedrockLLM.__new__(BedrockLLM)
        llm.max_tool_rounds, llm.model_id, llm.max_tokens, llm.temperature = 3, "m", 100, 0
        llm.client = FakeClient()
        conv = llm.new_conversation("sys", [Tool("get_anomalies", "d")])
        self.assertEqual(asyncio.run(conv.ask("go", self._tool)), '{"severity":"high"}')
        self.assertIn("toolConfig", calls[0])
        tool_result = calls[1]["messages"][2]["content"][0]["toolResult"]
        self.assertEqual(tool_result["toolUseId"], "t1")
        self.assertEqual(tool_result["content"][0]["json"]["seen"], "get_anomalies")

    def test_bedrock_failure_rolls_back_history(self):
        from mcp_streaming.llm import BedrockLLM

        class Broken:
            def converse(self, **kw):
                raise RuntimeError("throttled")

        llm = BedrockLLM.__new__(BedrockLLM)
        llm.max_tool_rounds, llm.model_id, llm.max_tokens, llm.temperature = 3, "m", 100, 0
        llm.client = Broken()
        conv = llm.new_conversation("sys", [])
        with self.assertRaises(LLMError):
            asyncio.run(conv.ask("go", self._tool))
        self.assertEqual(conv.messages, [])

    def test_openai_tool_round_trip(self):
        from mcp_streaming.llm import OpenAICompatLLM
        llm = OpenAICompatLLM(model="m", base_url="http://127.0.0.1:1/v1", api_key=None)
        payloads = []

        def fake_post(payload):
            payloads.append(json.loads(json.dumps(payload)))
            if len(payloads) == 1:
                return {"choices": [{"message": {"role": "assistant", "content": None, "tool_calls": [
                    {"id": "c1", "type": "function", "function": {"name": "get_anomalies", "arguments": "{}"}}]}}]}
            return {"choices": [{"message": {"role": "assistant", "content": "all good"}}]}

        llm._post = fake_post
        conv = llm.new_conversation("sys", [Tool("get_anomalies", "d")])
        self.assertEqual(asyncio.run(conv.ask("go", self._tool)), "all good")
        self.assertEqual(payloads[0]["tools"][0]["function"]["name"], "get_anomalies")
        tool_msg = payloads[1]["messages"][-1]
        self.assertEqual((tool_msg["role"], tool_msg["tool_call_id"]), ("tool", "c1"))


class FormatTest(unittest.TestCase):
    def test_format(self):
        out = format_diagnosis({"severity": "high", "summary": "s", "root_cause": "r",
                                "affected_services": ["a"], "remediation": ["x", "y"], "confidence": 0.8})
        self.assertIn("HIGH", out)
        self.assertIn("2. y", out)


if __name__ == "__main__":
    unittest.main()
