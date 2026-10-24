# Contributing to MCP Pulse

Thanks for your interest! This project grows through **forks and pull requests** — please fork rather than downloading a ZIP, so your copy stays connected to updates and your improvements can flow back.

## Workflow

1. **Fork** — click [Fork](https://github.com/kohli6/mcp-pulse/fork), or:
   ```bash
   gh repo fork kohli6/mcp-pulse --clone && cd mcp-pulse
   ```
   To work on an industry edition, untick **"Copy the main branch only"** so you also get the `usecase/*` branches.
2. **Branch** from an up-to-date `main` (or the relevant `usecase/<name>` branch):
   ```bash
   gh repo sync <your-user>/mcp-pulse && git pull
   git checkout -b feature/<short-description>
   ```
3. **Set up and test**:
   ```bash
   make install && source .venv/bin/activate
   make test            # must pass — offline, no Docker, no LLM needed
   make demo-offline    # quick end-to-end check
   ```
4. **Open a pull request** from your fork to `kohli6/mcp-pulse`. Describe what changed, why, and how you tested it.

## Guidelines

- **Keep it runnable offline.** New features should work with `LLM_PROVIDER=mock` and the in-process mock Kafka, and come with tests in `tests/`.
- **Security defaults stay on.** Servers bind to `127.0.0.1` by default, the Cedar auth stub fails closed, and the agent's tools stay read-only. Don't weaken these to make a demo easier.
- **No real data or secrets.** Use synthetic data only; never commit credentials, API keys or `.env` files.
- **Pin dependencies** to exact versions in `requirements*.txt`.
- **Docs move with code.** Update the README, FAQ and — for use cases — the deck (`make deck`) in the same pull request.

## Use cases (industry editions)

Each use case lives on its own branch `usecase/<name>` (the source of truth) and is mirrored on `main` under `use-cases/<name>/`.

- Make use-case changes on the **`usecase/<name>` branch**, not in the `use-cases/` folder.
- After a use-case PR merges, the maintainer refreshes the folder with `make sync-use-cases`; `make check-use-cases` verifies it.
- Proposing a new industry? Open an issue first, then follow *Adding a new use case* in [use-cases/README.md](use-cases/README.md).

## License of contributions

By submitting a pull request you agree that your contribution is licensed under the project's [Apache License 2.0](LICENSE) (see Section 5 of the license).

## Questions and ideas

Open a [GitHub issue](https://github.com/kohli6/mcp-pulse/issues) — bug reports, questions and use-case ideas are all welcome.
