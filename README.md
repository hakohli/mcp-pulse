# MCP Live: Streaming Context to AI Agents

A working demo of **streaming MCP (Model Context Protocol)** — pushing real-time context to AI agents using Kafka, WebSockets, and Cedar authorization.

Instead of the traditional request/response MCP model where agents poll for stale snapshots, this demo shows agents receiving live events as they happen, detecting anomalies in under 1 second, and suggesting fixes automatically.

**AI-driven:** an LLM agent (`ai_agent.py`) investigates every spike on its own — it calls the server's MCP methods as tools, correlates errors across services, and returns a structured diagnosis (severity, root cause, affected services, remediation). You can also chat with it while the stream runs, and the log simulator can let an LLM invent realistic incident scenarios. Providers: **Amazon Bedrock** (default), any **OpenAI-compatible** endpoint (OpenAI, Ollama, vLLM), or an offline **mock**.

## Architecture

```mermaid
graph TB
    subgraph Event Sources
        LS[log_simulator.py<br/>Generates realistic<br/>app logs + error spikes]
    end

    subgraph Message Bus
        K[Apache Kafka<br/>Topic: app-logs<br/>Confluent 7.6.0 / KRaft]
    end

    subgraph Streaming MCP Server
        KC[Kafka Consumer<br/>Rolling 100-event window]
        AD[Anomaly Detection<br/>Error rate tracking]
        WS[WebSocket Server<br/>Port 8765]
        CE[Cedar Policy Engine<br/>permit / forbid rules]
        KC --> AD
        KC --> WS
        CE --> WS
    end

    subgraph AI Agent
        SUB[Subscribe to stream]
        MON[Sliding window monitor<br/>20-event error rate]
        LLM[LLM reasoning loop<br/>Bedrock / OpenAI / Ollama<br/>tool calling]
        FIX[Structured diagnosis<br/>root cause + remediation]
        SUB --> MON -- spike --> LLM --> FIX
    end

    LS -- produce --> K
    K -- consume --> KC
    WS -- "push: live events" --> SUB
    LLM -- "tool call: get_anomalies() / get_context()" --> WS
    WS -- "tool result" --> LLM

    style K fill:#FF8C00,color:#fff
    style WS fill:#00D4AA,color:#000
    style CE fill:#FF4444,color:#fff
    style AD fill:#4488FF,color:#fff
    style FIX fill:#00D4AA,color:#000
    style LLM fill:#9B59B6,color:#fff
```

### Data Flow

```
log_simulator.py [--ai] → Kafka (app-logs) → streaming_mcp_server.py → ai_agent.py ⇄ LLM
                                                       ↕ WebSocket          (tool calls)
                                                 subscribe / get_anomalies / get_context
```

## What You'll See

- Normal log traffic flowing through the agent in real-time
- Every ~30 seconds, an error spike hits
- The agent detects the spike locally, then hands it to the LLM
- The LLM pulls context from the server through MCP tool calls (hybrid protocol) and prints an AI diagnosis
- In interactive mode you can ask "what's failing right now?" and get answers grounded in live data

## Why This Matters

### The problem with today's AI agents
Most AI agents — and the Model Context Protocol in its common form — are **pull-based**: the agent asks for context, gets a snapshot, and reasons over it. That works for documents and databases, but not for systems that change every second. By the time a polling agent sees a failure, the snapshot is stale, the incident has spread, and a human has usually already been paged.

### What this project demonstrates
This project shows a different pattern: **streaming context into an LLM agent**, so the agent works from a live picture of the system instead of a stale snapshot.

| Shift | From | To |
|-------|------|----|
| Context delivery | Agent polls for snapshots | Events are **pushed** to the agent as they happen |
| Detection | Static thresholds and dashboards a human watches | Agent detects anomalies in under a second, on the live stream |
| Diagnosis | Engineer correlates logs across services by hand | LLM **investigates autonomously** — pulls more context through MCP tool calls, separates root cause from symptoms |
| Remediation | Runbook lookup | Structured, ordered remediation steps produced in seconds |
| Governance | Shared credentials, all-or-nothing access | **Policy-controlled** access (Cedar) per agent role, per stream, per action |
| Coupling | Point-to-point integrations | **Kafka** as a durable, replayable, fan-out event backbone |

### Why it's significant
- **It extends MCP from request/response to real-time streaming.** The same WebSocket carries both the push stream and on-demand tool calls (the "hybrid protocol"), so agents get freshness *and* depth without separate channels.
- **It puts the LLM in the right place.** A cheap, deterministic detector decides *when* to think; the LLM decides *what* is wrong. This keeps LLM cost and latency off the hot path — the pattern that makes AI viable on high-volume streams.
- **It treats safety and governance as part of the design, not an afterthought.** Read-only tools, untrusted-input handling against prompt injection from log content, fail-closed authentication, policy-as-code authorization, and loopback-by-default networking are what regulated industries need before they let an agent near production data.
- **It is vendor-neutral and portable.** Swap the LLM (Bedrock, OpenAI, Ollama/on-prem), swap the event source (anything that writes JSON to Kafka), and the architecture holds — including fully on-premises deployments for data-sovereignty requirements.
- **It shortens the gap between "something broke" and "here's what to do"** from minutes or hours to seconds, which translates directly into lower downtime cost, fewer customers affected, and less on-call fatigue.

### Sectors and use cases

| Sector | Example streaming sources | What an AI streaming agent can do |
|--------|---------------------------|-----------------------------------|
| **Automotive & connected vehicles** | Vehicle telemetry (CAN bus, OBD-II), OTA update logs, EV charging networks, factory line sensors | Detect fleet-wide fault patterns after an OTA release and recommend a rollback; diagnose charging-station outages; flag predictive-maintenance issues before breakdowns |
| **Auto insurance** | Telematics / usage-based-insurance (UBI) feeds, crash-detection events, first-notice-of-loss (FNOL) intake, claims systems | Trigger instant FNOL from crash telemetry; spot staged-accident or claims-fraud patterns as they emerge; monitor UBI pipelines so pricing data isn't silently lost |
| **Health insurance** | Claims adjudication streams, eligibility/prior-authorization APIs, provider network systems, member portals | Detect claims-processing backlogs or denial-rate spikes in real time; flag anomalous billing patterns (upcoding, duplicate claims) for investigators; keep prior-auth systems within regulatory turnaround times |
| **Healthcare & life sciences** | Patient monitoring devices, EHR integration (HL7/FHIR) feeds, lab systems, pharmacy and supply systems | Catch failed HL7/FHIR interfaces before results go missing; correlate device alarms; monitor cold-chain for vaccines and biologics — with a human always in the loop for clinical decisions |
| **Banking & financial services** | Payment rails, card authorizations, trading platforms, core-banking logs, market data | Detect payment failures or authorization-decline spikes within seconds; real-time fraud and AML signal triage; diagnose trading-system latency before it affects order execution |
| **Telecommunications** | Network element logs, 5G core / RAN telemetry, CDRs, OSS/BSS events | Correlate alarm storms to a single root-cause node; predict cell-site or fiber outages; diagnose provisioning failures and dropped-call spikes; support autonomous-network (zero-touch) operations |
| **Energy & utilities** | Smart meters, SCADA/grid telemetry, substation sensors, renewable generation feeds | Detect grid anomalies and equipment degradation early; diagnose smart-meter data-collection failures; support outage response by correlating field events |
| **Manufacturing & Industry 4.0** | PLC/IoT sensor streams, MES logs, quality-inspection systems | Identify the upstream machine causing a quality defect; predictive maintenance; reduce unplanned line downtime |
| **Retail & e-commerce** | Checkout and payment services, inventory systems, clickstream, POS | Catch checkout failures during peak events (e.g., flash sales) before revenue is lost; detect inventory-sync drift between stores and online |
| **Logistics & supply chain** | Fleet GPS, warehouse robotics, shipment tracking, EDI feeds | Detect routing or scanning failures; diagnose warehouse automation stoppages; proactively flag shipments at risk of SLA breach |
| **Cybersecurity (SOC)** | SIEM events, firewall/IDS logs, authentication logs, EDR telemetry | Triage alert floods, correlate multi-stage attacks, and draft incident summaries for analysts — reducing mean time to detect and respond |
| **Cloud, SaaS & DevOps / SRE** | Application logs, deployment events, metrics, traces | Autonomous incident triage (this demo's core scenario); link a spike to the deployment that caused it; draft post-incident timelines |
| **Aviation & transportation** | Aircraft health monitoring, airport operations systems, rail signalling | Early detection of component degradation; diagnose baggage, check-in, or signalling system failures that cascade into delays |
| **Public sector & emergency services** | 911/CAD dispatch systems, traffic sensors, public-service portals | Keep critical citizen-facing systems available; correlate sensor events during emergencies to support responders |
| **Media, gaming & streaming** | CDN logs, game-server telemetry, video-player QoE metrics | Detect buffering or matchmaking failures during live events; pinpoint the region or CDN edge at fault |

### Adoption considerations
This repository is a **reference implementation and demo**, not a production system. Adopting the pattern in a real environment — especially in regulated sectors — means adding:
- **Real authentication** in place of the fail-closed stub (e.g., OAuth2/OIDC or mTLS), and production-grade Cedar via the official SDK.
- **Data protection and compliance controls** appropriate to the domain — e.g., HIPAA for health data, PCI DSS for card data, SOX/GLBA for financial records, GDPR/data-residency rules — including PII redaction before data reaches the LLM.
- **Human-in-the-loop approval** for any action that changes systems; the agent here is deliberately read-only.
- **Evaluation and observability** for the LLM itself: track diagnosis accuracy, false positives, latency, and cost.
- **Model choice that fits your data boundaries** — managed (Amazon Bedrock), API-based, or self-hosted (Ollama/vLLM) for fully on-premises operation.

## Quick Start — one command, no Docker

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python run_demo.py --provider mock --duration 40   # offline smoke test, no LLM needed
python run_demo.py                                 # real LLM (Bedrock by default — see "Configure the LLM")
python run_demo.py --interactive --ai              # chat with the agent + LLM-generated incidents
```

`run_demo.py` runs the real MCP server, log simulator and AI agent in one process, connected through the in-process mock Kafka and a loopback WebSocket. The first spike hits after ~10s (`--first-spike N`). For the full multi-process setup on real Kafka, follow **Full Setup** below.

## Prerequisites

| Tool | Version Tested | Purpose |
|------|---------------|---------|
| Python | 3.9+ | Runtime for all components |
| Docker | 29.3+ | Runs Kafka container (not needed for `run_demo.py`) |
| Apache Kafka | 7.6.0 (Confluent) | Durable event streaming bus |
| confluent-kafka (Python) | 2.13.2 | Kafka producer/consumer client |
| websockets (Python) | 15.0.1 | WebSocket server & client |
| Cedar | — | Authorization policy language (embedded) |
| boto3 (Python) | 1.42.97 | Amazon Bedrock client (default LLM provider) |
| LLM access | — | One of: Amazon Bedrock, OpenAI-compatible API, local Ollama — or `mock` for none |

## Full Setup — real Kafka, separate terminals (run in order)

### 1. Start Kafka
```bash
docker compose up -d
```
Wait ~15 seconds for Kafka to fully initialize (KRaft controller election + broker startup). The broker is published on `127.0.0.1:9092` only — it has no authentication.

### 2. Install dependencies
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Every terminal below needs the virtualenv activated (`source .venv/bin/activate`).

### 2b. Configure the LLM
| Provider | Setup |
|----------|-------|
| `bedrock` (default) | AWS credentials in the standard chain with `bedrock:InvokeModel` on the model, and model access enabled in the Bedrock console. Optional: `AWS_REGION` (default `us-east-1`), `BEDROCK_MODEL_ID` (default `us.anthropic.claude-3-5-haiku-20241022-v1:0`). |
| `openai` | `OPENAI_API_KEY`, optional `OPENAI_MODEL` (default `gpt-4o-mini`), `OPENAI_BASE_URL`. |
| `ollama` | Local Ollama running; optional `OPENAI_MODEL` (default `llama3.1`). The model must support tool calling. |
| `mock` | Nothing — offline rule-based stand-in, good for tests and dry runs. |

```bash
export LLM_PROVIDER=bedrock   # or openai | ollama | mock
export AWS_REGION=us-east-1   # bedrock only
```
Every LLM-aware command also accepts `--provider` to override `LLM_PROVIDER`. Quick check that Bedrock access works:
```bash
python -c "import asyncio, llm; c=llm.create_llm().new_conversation('Be brief.'); print(asyncio.run(c.ask('Say OK', None)))"
```

### 3. Start the Streaming MCP Server
```bash
python streaming_mcp_server.py
```
You should see: `Streaming MCP Server running on ws://127.0.0.1:8765`

The server has no authentication, so it binds to loopback by default (`MCP_HOST` / `MCP_PORT` override it — only widen the bind behind a gateway that authenticates callers).

### 4. Start the Log Simulator (separate terminal)
```bash
python log_simulator.py
```
This produces realistic application logs to Kafka every second. Add `--ai` to have the LLM invent a new, causally coherent incident (root-cause service fails first, downstream services follow) for each spike:
```bash
python log_simulator.py --ai
```

### 5. Start the AI Agent (separate terminal)
```bash
python ai_agent.py                # autonomous LLM monitor
python ai_agent.py --interactive  # monitor + chat with the agent
```
The agent subscribes to the live stream; on each spike the LLM investigates and prints a diagnosis like:
```
  🚨 Spike: 35% errors in last 20 events — 🧠 bedrock:... investigating...

  ┌─ 🧠 AI DIAGNOSIS ────────────────────────────────────────
  │ 🔴 Severity:   CRITICAL   (confidence 80%)
  │ 🎯 Root cause: inventory-db refusing connections; order/payment timeouts are downstream symptoms
  │ 🧩 Affected:   inventory-db, order-service, payment-service
  │ 🔧 Remediation:
  │    1. ...
```
Useful flags: `--provider`, `--threshold 0.3`, `--window 20`, `--cooldown 30` (min seconds between LLM calls, controls cost), `--show all|errors|none`.

In interactive mode ask things like *"what is failing right now?"*, *"is payment-service healthy?"*, *"what should I do first?"* — the LLM answers using live `get_anomalies` / `get_context` tool calls and remembers the conversation. `status` and `quit` are built in.

The original rule-based agents (`agent_client.py`, `agent_client_interactive.py`) still work as a non-LLM baseline.

### 5b. Cedar-authorized server (optional)
Use `streaming_mcp_server_cedar.py` in step 3 instead to enforce Cedar policies — see [Cedar server and authentication](#cedar-server-and-authentication). It uses mock Kafka unless `USE_REAL_KAFKA=1` is set.

### 5c. Rule-based Interactive Agent (no LLM)
```bash
python agent_client_interactive.py
```
You can type commands while events stream:
- `anomalies` — query the server for anomaly summary
- `context` — get current server context
- `status` — show local event count and error window
- `help` — list commands
- `quit` — exit

### Full Setup in one terminal (tmux)
With Kafka running and the venv activated, start all three components in split panes:
```bash
tmux new-session -s mcp -d 'PYTHONUNBUFFERED=1 python3 streaming_mcp_server.py' \; \
  split-window -h 'sleep 2 && PYTHONUNBUFFERED=1 python3 log_simulator.py --ai' \; \
  split-window -v 'sleep 4 && PYTHONUNBUFFERED=1 python3 ai_agent.py --interactive' \; \
  attach
```

### Tip: Unbuffered Output
If output appears delayed, use `PYTHONUNBUFFERED=1`:
```bash
PYTHONUNBUFFERED=1 python ai_agent.py
```

## Environment Variables

| Variable | Default | Used by | Purpose |
|----------|---------|---------|---------|
| `LLM_PROVIDER` | `bedrock` | agent, simulator `--ai`, `run_demo.py` | `bedrock` \| `openai` \| `ollama` \| `mock` |
| `AWS_REGION` | `us-east-1` | bedrock | Bedrock region |
| `BEDROCK_MODEL_ID` | `us.anthropic.claude-3-5-haiku-20241022-v1:0` | bedrock | Model / inference profile |
| `OPENAI_API_KEY` | — | openai | API key (omit for local servers) |
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` (`http://127.0.0.1:11434/v1` for ollama) | openai, ollama | Endpoint |
| `OPENAI_MODEL` | `gpt-4o-mini` (`llama3.1` for ollama) | openai, ollama | Model name |
| `MCP_HOST` / `MCP_PORT` | `127.0.0.1` / `8765` | servers | WebSocket bind address |
| `USE_MOCK_KAFKA` | unset | `streaming_mcp_server.py`, `log_simulator.py` | `1` = in-process mock Kafka (set automatically by `run_demo.py`) |
| `USE_REAL_KAFKA` | unset | `streaming_mcp_server_cedar.py` | `1` = real Kafka (default is mock) |
| `MCP_AUTH_TEST_BYPASS` | unset | `streaming_mcp_server_cedar.py` | `1` = accept client-asserted roles (local demos/tests only) |

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `LLM setup failed` / `Bedrock converse failed: ... AccessDeniedException` | Credentials lack `bedrock:InvokeModel`, or model access isn't enabled for that model in the Bedrock console/region. |
| `Bedrock converse failed: ... ValidationException ... inference profile` | Use an inference-profile ID (`us.` prefix) or a model available in your `AWS_REGION`. |
| `Diagnosis failed: request to http://127.0.0.1:11434/v1 failed` | Ollama isn't running (`ollama serve`), or the model isn't pulled (`ollama pull llama3.1`). |
| Agent prints events but never diagnoses | The error rate must exceed `--threshold` over a full `--window`; diagnoses are rate-limited by `--cooldown`. |
| `Connect to ipv4#127.0.0.1:9092 failed` | Kafka isn't up — `docker compose up -d`, or use `run_demo.py` instead. |
| `subscribe failed: ... FORBIDDEN` / `UNAUTHENTICATED` | Cedar server — see the authentication section below. |

## Key Patterns Demonstrated

1. **Event subscription** — agent subscribes once, gets continuous updates via `subscribe` / `unsubscribe`
2. **Hybrid protocol** — streaming + request/response (`get_anomalies`, `get_context`) on same WebSocket
3. **Sliding window anomaly detection** — server tracks error rates in a rolling 100-event buffer; agent triggers at >30% error rate
4. **LLM-driven diagnosis** — a cheap local detector decides *when* to think; the LLM decides *what* is wrong by calling MCP methods as tools, then returns structured JSON (severity, root cause, affected services, remediation). Cooldown bounds LLM cost. Log content is treated as untrusted input in the system prompt, and the LLM's tools are read-only.
5. **Backpressure handling** — bounded 500-event queues per subscriber; slow consumers are dropped
6. **Cedar authorization** — declarative permit/forbid policies controlling which agents can subscribe to which streams. Denials come back to the LLM as tool errors it reasons around.
7. **Kafka as durable event bus** — replayable, decoupled, scalable; consumer groups for load balancing or fan-out

## Files

| File | Description |
|------|-------------|
| `streaming_mcp_server.py` | Core streaming MCP server — Kafka consumer + WebSocket push |
| `streaming_mcp_server_cedar.py` | Server variant with Cedar authorization |
| `run_demo.py` | **One-command end-to-end demo** — server + simulator + agent in one process on mock Kafka |
| `ai_agent.py` | **LLM agent** — autonomous spike investigation + interactive chat, MCP methods as LLM tools |
| `llm.py` | LLM provider layer (Bedrock Converse, OpenAI-compatible, mock) with a tool-calling loop |
| `agent_client.py` | Rule-based agent (no LLM baseline) |
| `agent_client_interactive.py` | Rule-based interactive agent (no LLM baseline) |
| `log_simulator.py` | Produces log events to Kafka; `--ai` for LLM-generated incidents |
| `cedar_authz.py` | Cedar policy engine for MCP action authorization |
| `mock_kafka.py` | In-process mock Kafka for testing without Docker |
| `test_integration.py` | Integration tests (Cedar server + mock Kafka) |
| `test_ai_agent.py` | LLM/agent tests — offline (mock LLM, fake Bedrock/OpenAI transports) |
| `docker-compose.yml` | Kafka (Confluent cp-kafka 7.6.0, KRaft mode) |

## Tests

```bash
python -m unittest test_ai_agent -v                  # no Docker, no LLM, no network
python test_integration.py                           # Cedar server + mock Kafka
python run_demo.py --provider mock --duration 40     # end-to-end smoke test
```

## Cedar server and authentication

`streaming_mcp_server_cedar.py` evaluates Cedar policies against the connection's principal. Roles must come from a credential the server can **verify** — `verify_principal()` is a fail-closed stub that rejects every `auth` message until you wire a real verifier (for example, a JWT validated against your identity provider's JWKS, with the role taken from its claims). Until then every connection is the anonymous `viewer`.

For local demos and tests only, `MCP_AUTH_TEST_BYPASS=1` accepts the client-asserted principal:
```bash
MCP_AUTH_TEST_BYPASS=1 python streaming_mcp_server_cedar.py
python ai_agent.py --auth-role ops-agent
```

## Teardown

```bash
# Stop Python processes
pkill -f "streaming_mcp_server.py"
pkill -f "log_simulator.py"
pkill -f "agent_client.py"
pkill -f "ai_agent.py"
pkill -f "run_demo.py"

# Stop Kafka
docker compose down -v
```

## License

All open source: Apache Kafka, Cedar, WebSockets, Python. LLM providers (Amazon Bedrock, OpenAI, Ollama) are used under their own terms.
