"""
One-command end-to-end demo — no Docker, no Kafka install.

Runs the real streaming MCP server, the log simulator and the LLM agent in a
single process, connected through the in-process mock Kafka and a loopback
WebSocket. Same code paths as the multi-terminal setup; only the bus is mocked.

  python -m mcp_streaming.demo                               # Bedrock (default provider)
  python -m mcp_streaming.demo --provider mock               # fully offline
  python -m mcp_streaming.demo --interactive --ai            # chat + LLM-generated incidents
  python -m mcp_streaming.demo --provider mock --duration 40
"""

import argparse
import asyncio
import os
import sys
import threading

os.environ["USE_MOCK_KAFKA"] = "1"  # must be set before importing server/simulator

import websockets  # noqa: E402

from mcp_streaming.agents import ai_agent  # noqa: E402
from mcp_streaming.simulator import log_simulator  # noqa: E402
from mcp_streaming.llm import LLMError  # noqa: E402
from mcp_streaming.server.streaming_mcp_server import LogStreamConsumer, StreamingMCPServer, kafka_poll_loop  # noqa: E402


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Run server + simulator + AI agent in one process (mock Kafka)")
    p.add_argument("--provider", default=None, help="bedrock | openai | ollama | mock (default: $LLM_PROVIDER or bedrock)")
    p.add_argument("--interactive", "-i", action="store_true", help="chat with the agent while it monitors")
    p.add_argument("--ai", action="store_true", help="let the LLM generate incident scenarios in the simulator")
    p.add_argument("--duration", type=float, default=0, help="stop after N seconds (0 = run until Ctrl+C)")
    p.add_argument("--first-spike", type=int, default=10, help="seconds until the first error spike (default 10)")
    p.add_argument("--show", choices=["all", "errors", "none"], help="which streamed events the agent prints")
    p.add_argument("--port", type=int, default=0, help="loopback port for the MCP server (default: random free port)")
    return p.parse_args(argv)


async def main(args):
    # 1. Streaming MCP server on loopback
    log_stream = LogStreamConsumer()
    server = StreamingMCPServer(log_stream)
    ws_server = await websockets.serve(server.handle_client, "127.0.0.1", args.port)
    port = ws_server.sockets[0].getsockname()[1]
    poll = asyncio.ensure_future(kafka_poll_loop(log_stream))
    print(f"🛰  Streaming MCP Server on ws://127.0.0.1:{port} (mock Kafka)")

    # 2. Log simulator in a background thread (it is a blocking 1s-tick loop)
    ai = log_simulator.AIScenarioGenerator(args.provider) if args.ai else None
    start_tick = max(0, 30 - args.first_spike)
    threading.Thread(target=log_simulator.run, args=(ai, start_tick), daemon=True).start()
    await asyncio.sleep(0.5)

    # 3. LLM agent
    agent_argv = ["--uri", f"ws://127.0.0.1:{port}"]
    if args.provider:
        agent_argv += ["--provider", args.provider]
    if args.interactive:
        agent_argv.append("--interactive")
    if args.show:
        agent_argv += ["--show", args.show]
    agent_task = asyncio.ensure_future(ai_agent.run(ai_agent.parse_args(agent_argv)))

    try:
        if args.duration > 0:
            done, _ = await asyncio.wait({agent_task}, timeout=args.duration)
            if done:
                agent_task.result()
            else:
                print(f"\n⏱  {args.duration:.0f}s elapsed — stopping demo.")
        else:
            await agent_task
    finally:
        agent_task.cancel()
        poll.cancel()
        ws_server.close()
        await ws_server.wait_closed()


if __name__ == "__main__":
    print("🚀 MCP Live — end-to-end AI demo (single process)")
    print("=" * 50)
    try:
        asyncio.run(main(parse_args()))
    except KeyboardInterrupt:
        pass
    except LLMError as exc:
        sys.exit(f"LLM setup failed: {exc}")
    sys.stdout.flush()  # os._exit skips normal interpreter cleanup, including buffer flushes
    sys.stderr.flush()
    os._exit(0)  # don't wait on the simulator's daemon thread / executor
