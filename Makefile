# Short aliases for the common commands. Run `make help` to list them.
# Uses the project virtualenv if present, otherwise python3 on PATH.
PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
export PYTHONUNBUFFERED := 1

.PHONY: help install demo demo-offline demo-chat kafka-up kafka-down server server-cedar simulator simulator-ai agent agent-chat test clean

help:  ## List available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-14s %s\n", $$1, $$2}'

install:  ## Create .venv and install pinned dependencies
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

demo:  ## One-process end-to-end demo with the configured LLM (no Docker)
	$(PYTHON) -m mcp_streaming.demo

demo-offline:  ## End-to-end demo with the offline mock LLM, stops after 40s
	$(PYTHON) -m mcp_streaming.demo --provider mock --duration 40

demo-chat:  ## End-to-end demo + interactive chat + LLM-generated incidents
	$(PYTHON) -m mcp_streaming.demo --interactive --ai

kafka-up:  ## Start Kafka in Docker (127.0.0.1:9092)
	docker compose up -d

kafka-down:  ## Stop Kafka and remove its volumes
	docker compose down -v

server:  ## Streaming MCP server on real Kafka
	$(PYTHON) -m mcp_streaming.server.streaming_mcp_server

server-cedar:  ## Cedar-authorized server (mock Kafka unless USE_REAL_KAFKA=1)
	$(PYTHON) -m mcp_streaming.server.streaming_mcp_server_cedar

simulator:  ## Log simulator -> Kafka (static incidents)
	$(PYTHON) -m mcp_streaming.simulator.log_simulator

simulator-ai:  ## Log simulator -> Kafka (LLM-generated incidents)
	$(PYTHON) -m mcp_streaming.simulator.log_simulator --ai

agent:  ## LLM agent (autonomous monitor)
	$(PYTHON) -m mcp_streaming.agents.ai_agent

agent-chat:  ## LLM agent with interactive chat
	$(PYTHON) -m mcp_streaming.agents.ai_agent --interactive

test:  ## Run all tests (offline: no Docker, no LLM, no network)
	$(PYTHON) -m unittest discover -s tests -t . -v

clean:  ## Remove caches
	find . -name __pycache__ -type d -prune -not -path './.venv/*' -exec rm -rf {} +
