"""
LLM provider layer for the Streaming MCP demo.

Every provider exposes the same tiny interface:

    llm = create_llm()                       # picks provider from LLM_PROVIDER
    conv = llm.new_conversation(system, tools)
    answer = await conv.ask("question", execute_tool)

`execute_tool(name, input_dict)` is an async callback the conversation invokes
whenever the model decides to call one of the tools. The conversation keeps its
own history, so follow-up `ask()` calls have memory.

Providers (LLM_PROVIDER env var):
  bedrock  (default) Amazon Bedrock Converse API via boto3. Uses the standard AWS
           credential chain. Model: BEDROCK_MODEL_ID. Region: AWS_REGION.
  openai   Any OpenAI-compatible /chat/completions endpoint (OpenAI, Ollama,
           vLLM, LM Studio). OPENAI_BASE_URL, OPENAI_MODEL, OPENAI_API_KEY.
  ollama   Shortcut for `openai` pointed at a local Ollama (127.0.0.1:11434).
  mock     Deterministic offline provider (rule-based). No network. Used by
           tests and for demos without LLM access.
"""

import asyncio
import json
import os
import re
import urllib.error
import urllib.request
from typing import Any, Awaitable, Callable, Dict, List, Optional

ToolExecutor = Callable[[str, Dict[str, Any]], Awaitable[Dict[str, Any]]]

DEFAULT_BEDROCK_MODEL = "us.anthropic.claude-3-5-haiku-20241022-v1:0"
DEFAULT_OPENAI_MODEL = "gpt-4o-mini"
DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"


class LLMError(Exception):
    """Raised when the model call fails or the tool loop does not converge."""


class Tool:
    """A tool the model may call. `schema` is a JSON Schema for the input object."""

    def __init__(self, name: str, description: str, schema: Optional[dict] = None):
        self.name = name
        self.description = description
        self.schema = schema or {"type": "object", "properties": {}}


async def _run_tool(execute_tool: ToolExecutor, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a tool and always return a JSON object (errors are reported to the model)."""
    try:
        result = await execute_tool(name, args or {})
    except Exception as exc:  # surface failures to the model instead of crashing the loop
        return {"error": f"{type(exc).__name__}: {exc}"}
    return result if isinstance(result, dict) else {"result": result}


def extract_json(text: str) -> Optional[Any]:
    """Best-effort extraction of a JSON object/array from model output."""
    if not text:
        return None
    fenced = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL)
    candidates = [fenced.group(1)] if fenced else []
    candidates.append(text)
    for cand in candidates:
        cand = cand.strip()
        try:
            return json.loads(cand)
        except ValueError:
            pass
        for open_c, close_c in (("{", "}"), ("[", "]")):
            start, end = cand.find(open_c), cand.rfind(close_c)
            if start != -1 and end > start:
                try:
                    return json.loads(cand[start:end + 1])
                except ValueError:
                    continue
    return None


# --------------------------------------------------------------------------- #
# Base classes
# --------------------------------------------------------------------------- #

class Conversation:
    def __init__(self, system: str, tools: List[Tool], max_tool_rounds: int):
        self.system = system
        self.tools = tools
        self.max_tool_rounds = max_tool_rounds

    async def ask(self, text: str, execute_tool: ToolExecutor) -> str:
        raise NotImplementedError


class LLM:
    name = "base"

    def __init__(self, max_tool_rounds: int = 6):
        self.max_tool_rounds = max_tool_rounds

    def new_conversation(self, system: str, tools: Optional[List[Tool]] = None) -> Conversation:
        raise NotImplementedError

    def describe(self) -> str:
        return self.name


# --------------------------------------------------------------------------- #
# Amazon Bedrock (Converse API)
# --------------------------------------------------------------------------- #

class BedrockLLM(LLM):
    name = "bedrock"

    def __init__(self, model_id: Optional[str] = None, region: Optional[str] = None,
                 max_tokens: int = 1024, temperature: float = 0.2, **kw):
        super().__init__(**kw)
        try:
            import boto3
            from botocore.config import Config
        except ImportError as exc:
            raise LLMError("boto3 is required for LLM_PROVIDER=bedrock (pip install boto3)") from exc
        self.model_id = model_id or os.environ.get("BEDROCK_MODEL_ID", DEFAULT_BEDROCK_MODEL)
        self.region = region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.client = boto3.client(
            "bedrock-runtime",
            region_name=self.region,
            config=Config(read_timeout=60, retries={"max_attempts": 3, "mode": "adaptive"}),
        )

    def describe(self) -> str:
        return f"bedrock:{self.model_id} ({self.region})"

    def new_conversation(self, system, tools=None):
        return _BedrockConversation(self, system, tools or [], self.max_tool_rounds)


class _BedrockConversation(Conversation):
    def __init__(self, llm: BedrockLLM, system, tools, max_tool_rounds):
        super().__init__(system, tools, max_tool_rounds)
        self.llm = llm
        self.messages: List[dict] = []

    def _tool_config(self) -> dict:
        return {"tools": [
            {"toolSpec": {"name": t.name, "description": t.description, "inputSchema": {"json": t.schema}}}
            for t in self.tools
        ]}

    async def ask(self, text, execute_tool):
        start = len(self.messages)
        self.messages.append({"role": "user", "content": [{"text": text}]})
        try:
            for _ in range(self.max_tool_rounds + 1):
                kwargs = {
                    "modelId": self.llm.model_id,
                    "messages": self.messages,
                    "system": [{"text": self.system}],
                    "inferenceConfig": {"maxTokens": self.llm.max_tokens, "temperature": self.llm.temperature},
                }
                if self.tools:
                    kwargs["toolConfig"] = self._tool_config()
                try:
                    resp = await asyncio.to_thread(self.llm.client.converse, **kwargs)
                except Exception as exc:
                    raise LLMError(f"Bedrock converse failed: {exc}") from exc

                message = resp["output"]["message"]
                self.messages.append(message)
                tool_uses = [c["toolUse"] for c in message.get("content", []) if "toolUse" in c]
                if resp.get("stopReason") != "tool_use" or not tool_uses:
                    return "".join(c.get("text", "") for c in message.get("content", [])).strip()

                results = []
                for tu in tool_uses:
                    out = await _run_tool(execute_tool, tu["name"], tu.get("input") or {})
                    results.append({"toolResult": {"toolUseId": tu["toolUseId"], "content": [{"json": out}]}})
                self.messages.append({"role": "user", "content": results})
            raise LLMError(f"model exceeded {self.max_tool_rounds} tool rounds")
        except BaseException:
            del self.messages[start:]  # keep history valid for the next ask()
            raise


# --------------------------------------------------------------------------- #
# OpenAI-compatible (/chat/completions) — OpenAI, Ollama, vLLM, LM Studio ...
# --------------------------------------------------------------------------- #

class OpenAICompatLLM(LLM):
    name = "openai"

    def __init__(self, model: Optional[str] = None, base_url: Optional[str] = None,
                 api_key: Optional[str] = None, max_tokens: int = 1024, temperature: float = 0.2, **kw):
        super().__init__(**kw)
        self.model = model or os.environ.get("OPENAI_MODEL", DEFAULT_OPENAI_MODEL)
        self.base_url = (base_url or os.environ.get("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL)).rstrip("/")
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self.max_tokens = max_tokens
        self.temperature = temperature

    def describe(self) -> str:
        return f"openai:{self.model} ({self.base_url})"

    def new_conversation(self, system, tools=None):
        return _OpenAIConversation(self, system, tools or [], self.max_tool_rounds)

    def _post(self, payload: dict) -> dict:
        req = urllib.request.Request(
            f"{self.base_url}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json",
                     **({"Authorization": f"Bearer {self.api_key}"} if self.api_key else {})},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")[:500]
            raise LLMError(f"HTTP {exc.code} from {self.base_url}: {body}") from exc
        except (urllib.error.URLError, OSError, ValueError) as exc:
            raise LLMError(f"request to {self.base_url} failed: {exc}") from exc


class _OpenAIConversation(Conversation):
    def __init__(self, llm: OpenAICompatLLM, system, tools, max_tool_rounds):
        super().__init__(system, tools, max_tool_rounds)
        self.llm = llm
        self.messages: List[dict] = [{"role": "system", "content": system}]

    async def ask(self, text, execute_tool):
        start = len(self.messages)
        self.messages.append({"role": "user", "content": text})
        try:
            for _ in range(self.max_tool_rounds + 1):
                payload = {
                    "model": self.llm.model,
                    "messages": self.messages,
                    "max_tokens": self.llm.max_tokens,
                    "temperature": self.llm.temperature,
                }
                if self.tools:
                    payload["tools"] = [
                        {"type": "function",
                         "function": {"name": t.name, "description": t.description, "parameters": t.schema}}
                        for t in self.tools
                    ]
                resp = await asyncio.to_thread(self.llm._post, payload)
                try:
                    message = resp["choices"][0]["message"]
                except (KeyError, IndexError, TypeError) as exc:
                    raise LLMError(f"unexpected response shape: {str(resp)[:300]}") from exc

                assistant = {"role": "assistant", "content": message.get("content") or ""}
                tool_calls = message.get("tool_calls") or []
                if tool_calls:
                    assistant["tool_calls"] = tool_calls
                self.messages.append(assistant)
                if not tool_calls:
                    return (message.get("content") or "").strip()

                for call in tool_calls:
                    fn = call.get("function", {})
                    try:
                        args = json.loads(fn.get("arguments") or "{}")
                    except ValueError:
                        args = {}
                    out = await _run_tool(execute_tool, fn.get("name", ""), args)
                    self.messages.append({"role": "tool", "tool_call_id": call.get("id"), "content": json.dumps(out)})
            raise LLMError(f"model exceeded {self.max_tool_rounds} tool rounds")
        except BaseException:
            del self.messages[start:]
            raise


# --------------------------------------------------------------------------- #
# Mock provider — offline, deterministic, still exercises the tool-calling loop
# --------------------------------------------------------------------------- #

MOCK_REMEDIATIONS = {
    "Connection refused": ("Target service is down or unreachable",
                           ["Check the target service is running", "Verify security groups / network ACLs"]),
    "Timeout waiting": ("Downstream service is slow or saturated",
                        ["Check downstream health and latency", "Add a circuit breaker and tune timeouts"]),
    "OutOfMemoryError": ("JVM heap exhaustion",
                         ["Increase heap (-Xmx) or pod memory limit", "Capture a heap histogram to look for leaks"]),
    "SSL handshake failed": ("Expired TLS certificate",
                             ["Renew the certificate", "Add certificate-expiry alarms"]),
    "Deadlock detected": ("Lock-ordering conflict in DB transactions",
                          ["Review transaction isolation / lock order", "Consider optimistic locking"]),
    "Disk usage critical": ("Log volume filling up",
                            ["Rotate/vacuum old logs", "Expand the volume"]),
}


class MockLLM(LLM):
    name = "mock"

    def new_conversation(self, system, tools=None):
        return _MockConversation(system, tools or [], self.max_tool_rounds)


class _MockConversation(Conversation):
    async def ask(self, text, execute_tool):
        names = {t.name for t in self.tools}
        anomalies: Dict[str, Any] = {}
        if "get_anomalies" in names:
            anomalies = await _run_tool(execute_tool, "get_anomalies", {})

        top = anomalies.get("top_errors") or []
        if not top:
            if "JSON" not in text:
                return "[mock] No significant errors in the current window — the system looks healthy."
            return json.dumps({"severity": "low", "root_cause": "No significant errors in the current window.",
                               "affected_services": [], "remediation": [], "confidence": 0.5,
                               "summary": "System looks healthy."})

        lead = top[0]["message"]
        cause, steps = "Unknown error pattern", ["Investigate service logs and recent deployments"]
        for pattern, (c, s) in MOCK_REMEDIATIONS.items():
            if pattern in lead:
                cause, steps = c, s
                break
        rate = anomalies.get("error_rate", 0)
        severity = "critical" if rate >= 0.5 else "high" if rate >= 0.3 else "medium"
        if "JSON" not in text:  # chat question, not a structured diagnosis request
            return (f"[mock] Error rate is {rate:.0%} over the last {anomalies.get('window_size', '?')} events. "
                    f"Most frequent error: '{lead}' ({top[0].get('count')}x). Likely cause: {cause}. "
                    f"Suggested next step: {steps[0]}.")
        return json.dumps({
            "severity": severity,
            "root_cause": f"{cause} (most frequent: '{lead}')",
            "affected_services": [],
            "remediation": steps,
            "confidence": 0.6,
            "summary": f"Error rate {rate:.0%}; dominant error: {lead}",
        })


# --------------------------------------------------------------------------- #
# Factory
# --------------------------------------------------------------------------- #

def create_llm(provider: Optional[str] = None, **kwargs) -> LLM:
    provider = (provider or os.environ.get("LLM_PROVIDER", "bedrock")).strip().lower()
    if provider == "bedrock":
        return BedrockLLM(**kwargs)
    if provider == "openai":
        return OpenAICompatLLM(**kwargs)
    if provider == "ollama":  # local Ollama exposes an OpenAI-compatible API
        kwargs.setdefault("base_url", os.environ.get("OPENAI_BASE_URL", "http://127.0.0.1:11434/v1"))
        kwargs.setdefault("model", os.environ.get("OPENAI_MODEL", "llama3.1"))
        return OpenAICompatLLM(**kwargs)
    if provider == "mock":
        return MockLLM(**kwargs)
    raise LLMError(f"unknown LLM_PROVIDER '{provider}' (expected bedrock | openai | ollama | mock)")
