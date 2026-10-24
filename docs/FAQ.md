# MCP Live Streaming Demo — FAQ

## General

**1. What is this project?**
A working demo of streaming MCP (Model Context Protocol) — instead of the traditional request/response model where agents poll for stale snapshots, this pushes real-time log events to AI agents via Kafka + WebSockets. Agents detect anomalies in under a second and suggest fixes automatically.

**2. Who is this for?**
Anyone interested in extending MCP beyond request/response — particularly developers building AI agents that need live, continuous context rather than point-in-time snapshots.

**3. What problem does it solve?**
Standard MCP is pull-based: agents ask for context and get a static snapshot. In operational scenarios (monitoring, incident response), context goes stale immediately. This demo shows how to push events to agents as they happen.

---

## Architecture

**4. What's the data flow?**
`simulator/log_simulator.py` → Kafka (`app-logs` topic) → `server/streaming_mcp_server.py` → WebSocket → `agents/ai_agent.py` ⇄ LLM  (all under `mcp_streaming/`)

The LLM never talks to Kafka or the server directly — the agent exposes the server's MCP methods (`get_anomalies`, `get_context`) to the model as tools, and executes the model's tool calls over the same WebSocket.

**5. Why Kafka instead of just WebSockets end-to-end?**
Kafka provides durability, replayability, and decoupling. If the MCP server restarts, events aren't lost. Multiple consumers can independently read the same stream. You also get consumer groups for load balancing or fan-out.

**6. What's the "hybrid protocol"?**
The agent uses a single WebSocket connection for both streaming (continuous event push) and request/response (on-demand queries like `get_anomalies` and `get_context`). This avoids needing separate channels for real-time data vs. ad-hoc queries.

**7. How does anomaly detection work?**
Two levels:
- **Server-side**: Maintains a rolling 100-event window and tracks error rates + top error patterns via `get_anomaly_summary()`.
- **Agent-side**: Tracks a 20-event sliding window. When the error rate exceeds 30% (and the cooldown has passed), it hands the incident to the LLM, which queries the server via tool calls and returns a structured diagnosis.

---

## Authorization

**8. What is Cedar and why is it here?**
Cedar is a policy language (created by AWS) for fine-grained authorization. The demo uses it to control which agents can subscribe to which streams and call which methods — e.g., only `ops-agent` roles can call `get_anomalies`, only `senior-ops` can call `get_context`.

**9. What are the default Cedar policies?**
- Any authenticated agent can subscribe to `app-logs`
- Only `ops-agent` role can request anomaly summaries
- Only `senior-ops` role can access the full context window
- `readonly` agents are forbidden from subscribing to `audit-logs`
- Default deny — if no policy matches, access is denied

**10. Is the Cedar integration production-ready?**
No. `mcp_streaming/server/cedar_authz.py` is a lightweight regex-based parser for demo purposes. In production, you'd use the official `cedarpy` SDK or the Cedar service for policy evaluation.

---

## Running the Demo

**11. What are the prerequisites?**
Python 3.9+ and `pip install -r requirements.txt`. Docker is only needed for the full real-Kafka setup. For real AI diagnoses you need one LLM provider: Amazon Bedrock (AWS credentials + model access), an OpenAI-compatible API, or a local Ollama. `--provider mock` runs with no LLM at all.

**12. Do I need a real Kafka cluster?**
No. `make demo` (or `python -m mcp_streaming.demo`) runs everything in one process on `mcp_streaming/messaging/mock_kafka.py`, an in-process drop-in replacement. For the full multi-process demo, a single-node Kafka runs via `make kafka-up` / `docker compose up -d` (Confluent cp-kafka 7.6.0 in KRaft mode, no ZooKeeper), published on `127.0.0.1:9092` only.

**13. Which agent should I run?**
- `mcp_streaming/agents/ai_agent.py` (`make agent` / `make agent-chat`) — the LLM agent. Autonomous spike investigation; add `--interactive` to ask it questions in plain English mid-stream.
- `mcp_streaming/agents/agent_client.py` — rule-based baseline (static error → fix lookup). No LLM, no user input.
- `mcp_streaming/agents/agent_client_interactive.py` — rule-based baseline with typed commands (`anomalies`, `context`, `status`, `help`, `quit`).

**14. Output looks delayed/buffered — what do I do?**
Prefix the command with `PYTHONUNBUFFERED=1` (e.g. `PYTHONUNBUFFERED=1 python -m mcp_streaming.agents.ai_agent`) to disable Python's output buffering. The `make` targets already set it.

**15. How do I run everything in one terminal?**
`make demo` — server, simulator and agent in a single process. For the real-Kafka setup, use the tmux one-liner from the README (three panes: server, simulator, agent).

**15b. Where does everything live, and how do I run a single component?**
All code is in the `mcp_streaming/` package, grouped by role: `server/` (MCP servers + Cedar), `agents/` (LLM agent + rule-based baselines), `llm/` (providers), `simulator/` (log producer), `messaging/` (mock Kafka). Tests are in `tests/`, docs in `docs/`. Run any component from the repository root with `python -m mcp_streaming.<folder>.<module>`, or use the matching `make` target (`make help` lists them). See *Project Structure* in the README.

---

## Design Patterns

**16. How does backpressure work?**
Each subscriber gets a bounded `asyncio.Queue` (max 500 events). If a slow consumer's queue fills up, it's automatically dropped from the subscriber set. This prevents one slow agent from blocking the entire server.

**17. What error patterns does the agent recognize?**
With a real LLM, any — the model reasons over whatever the logs say, including the novel incidents `make simulator-ai` generates. The rule-based agents and the `mock` provider recognize six patterns with mapped remediation suggestions:
- `Connection refused` → check service status, security groups
- `Timeout waiting` → increase timeout, add circuit breaker
- `OutOfMemoryError` → increase heap, check for leaks
- `SSL handshake failed` → renew certificate
- `Deadlock detected` → review isolation levels
- `Disk usage critical` → clean logs, expand volume

**18. How are error spikes simulated?**
`mcp_streaming/simulator/log_simulator.py` runs on a 45-second cycle. Between ticks 30–40, it injects 3–6 ERROR events per second (with a 30% chance of a FATAL). Outside that window, it's normal INFO traffic with occasional WARNs. With `--ai`, an LLM pre-generates a causally ordered incident (root-cause service fails first, downstream services follow) ~25s before each spike, and the simulator replays it; if the LLM is unavailable it falls back to the static messages.

---

## Extending the Demo

**19. Can I add my own log sources?**
Yes — anything that produces JSON to the `app-logs` Kafka topic will flow through. Match the schema: `{"timestamp", "level", "service", "message", "request_id"}`.

**20. Can I add new Cedar policies?**
Add policy strings to the `CEDAR_POLICIES` list in `mcp_streaming/server/cedar_authz.py`. Use `mcp_streaming/server/streaming_mcp_server_cedar.py` (`make server-cedar`) to enforce them.

**21. How is the LLM connected?**
`mcp_streaming/llm/providers.py` gives every provider the same interface (`create_llm().new_conversation(system, tools).ask(...)`) and runs the tool-calling loop. `mcp_streaming/agents/ai_agent.py` registers three tools — `get_anomalies`, `get_context` (with level/service/limit filters) and `get_local_stats` — and executes the model's calls over the existing WebSocket. The protocol is unchanged; the server doesn't know an LLM is involved. Switch providers with `LLM_PROVIDER` or `--provider`; add one by subclassing `LLM`/`Conversation` in `providers.py` and registering it in `create_llm()`.

**21b. How do I keep LLM costs and latency under control?**
The LLM is never on the hot path. A cheap local detector decides *when* to call it, `--cooldown` (default 30s) caps the call rate, each diagnosis uses a fresh short conversation, and `get_context` returns at most 100 filtered events. The default Bedrock model is Claude 3.5 Haiku for speed and cost.

**21c. Can log content manipulate the LLM (prompt injection)?**
Logs are untrusted input. The system prompt tells the model to treat log content as data and never follow instructions inside it, and all of the model's tools are read-only — the worst a malicious log line can do is skew a diagnosis, not take an action. If you add write-capable tools (restart a service, roll back a deploy), gate them behind human confirmation and Cedar policies.

---

## Advanced / Complex Questions

**22. How would this scale to thousands of concurrent agents?**
The current design has one WebSocket per agent, each with its own `asyncio.Queue`. At scale, this hits memory limits (500 events × N agents) and a single-threaded event loop bottleneck. To scale:
- Horizontally shard the MCP server behind a load balancer — use Kafka consumer groups so each server instance handles a partition subset.
- Replace per-agent queues with a pub/sub broker (Redis Streams, NATS) between Kafka and WebSocket layer.
- Consider Server-Sent Events (SSE) for one-way streaming to reduce connection overhead if agents don't need the hybrid request/response path.

**23. What happens if the MCP server crashes mid-stream? Do agents lose events?**
Yes — events in-flight in the asyncio queues are lost. However, Kafka retains the full log. On restart, the server's consumer group offset determines where it resumes. Since `auto.offset.reset` is set to `latest`, it skips events produced during downtime. To guarantee no event loss:
- Change `auto.offset.reset` to `earliest` and commit offsets manually after successful delivery to all subscribers.
- Agents should track the last event timestamp they received and request a replay via a `get_context` call on reconnect.

**24. The Kafka consumer uses `group.id: "mcp-streaming-server"`. What happens with multiple server instances?**
Kafka partitions the topic across consumers in the same group — each instance gets a subset of partitions. This means each agent only sees a fraction of events unless you either:
- Give each server instance a unique `group.id` (fan-out — every instance gets all events), or
- Use a single partition (no parallelism), or
- Add a coordination layer that merges partitioned streams before pushing to agents.

**25. The sliding window anomaly detection is purely count-based. How would you make it time-aware?**
The current 100-event window conflates high-throughput periods with low-throughput ones — 30% errors in 2 seconds is very different from 30% errors over 10 minutes. Improvements:
- Switch to a time-based window (e.g., last 60 seconds) using a deque with timestamp-based eviction.
- Use exponential weighted moving average (EWMA) for error rate to smooth out short bursts.
- Add rate-of-change detection — a sudden jump from 2% to 15% is more significant than a steady 20%.

**26. How would you handle multi-tenant isolation — multiple customers' logs on the same server?**
Currently there's one Kafka topic and one subscriber pool. For multi-tenancy:
- Use per-tenant Kafka topics (`app-logs-tenant-a`, `app-logs-tenant-b`) and have agents subscribe to their tenant's topic.
- Extend the Cedar policies to bind principals to tenant IDs: `permit(principal, action == Action::"subscribe", resource) when { principal.tenant == resource.tenant };`
- Add tenant context to the WebSocket handshake (JWT or API key) and validate before allowing subscription.

**27. The Cedar policy engine parses policies with regex. What breaks?**
Several things:
- Nested `when` clauses with boolean logic (`&&`, `||`) aren't handled.
- Set-based conditions (`principal.tags.containsAll(...)`) are ignored.
- The regex parser can't handle multi-line policies or comments.
- There's no schema validation — typos in entity types silently produce no-match rules (default deny).
- In production, use the official Cedar SDK which handles the full grammar, entity stores, and schema validation.

**28. Could this replace polling-based MCP for all use cases?**
No. Streaming is ideal for high-frequency, time-sensitive data (logs, metrics, alerts). But polling/request-response is simpler and better for:
- Low-frequency context (customer profiles, config snapshots) where freshness isn't critical.
- Large context payloads that would overwhelm a WebSocket stream.
- Stateless agents that spin up, grab context, and terminate.
The hybrid protocol in this demo is the pragmatic answer — stream what's time-sensitive, request/response for everything else.

**29. What's the failure mode if Kafka is down but the WebSocket server is still running?**
The `poll()` method returns `None` on Kafka errors, so the server stays alive but stops pushing events. Agents remain connected but see silence. There's no health signal — agents can't distinguish "no events happening" from "Kafka is dead." Fix:
- Add a heartbeat mechanism — server sends a `{"type": "heartbeat"}` every N seconds. If agents don't receive one, they know the pipeline is broken.
- Expose a `/health` endpoint on the server that checks Kafka connectivity.

**30. How would you add event replay — letting an agent request historical events?**
Currently `get_context` returns the in-memory 100-event window. For true replay:
- Store events in a durable secondary store (DynamoDB, S3, or Kafka with long retention).
- Add a `replay` method: `{"method": "replay", "params": {"from": "2026-04-24T10:00:00", "to": "2026-04-24T10:05:00"}}`.
- Stream replayed events with a `{"type": "replay_event"}` tag so agents can distinguish live from historical.
- Consider backpressure — replaying 10,000 events at once will blow the 500-event queue. Batch and pace delivery.

**31. How does the LLM agent avoid adding latency to the event stream?**
It never blocks the stream on an LLM call:
- The WebSocket reader keeps consuming and printing events; a spike schedules the diagnosis as a background asyncio task, and the blocking SDK/HTTP call runs in a worker thread.
- Only one diagnosis runs at a time, and `--cooldown` rate-limits them.
- Tool-call requests share the socket with the push stream; a lock keeps one request in flight so responses can't be confused with each other, while events keep flowing.
- The `mock` provider and the rule-based agents remain as a zero-latency fallback when no LLM is available.

**32. What ordering guarantees does this architecture provide?**
Kafka guarantees ordering within a partition. Since the demo uses a single-partition topic, events arrive in producer order. But:
- The asyncio queue per subscriber is FIFO, so ordering is preserved to the agent.
- If you scale to multiple partitions, ordering is only guaranteed per-partition. Events from different services could interleave differently per consumer.
- WebSocket delivery is ordered (TCP), so no reordering between server and agent.
- The weak link is the `notify_subscribers` fan-out — if one queue is near-full and another is empty, they'll diverge in how far behind they are.
