# Use Cases

Industry editions of the streaming-MCP + LLM agent demo. Each folder is a **complete, self-contained project** — its own code, tests, docs, slide deck and `Makefile` — that applies the domain-neutral pattern at the root of this repository to one industry.

| Use case | Folder | Branch (source of truth) | What it shows |
|----------|--------|--------------------------|---------------|
| 🚗 **Auto insurance** | [`auto-insurance/`](auto-insurance/) | [`usecase/auto-insurance`](https://github.com/kohli6/mcp-pulse/tree/usecase/auto-insurance) | An LLM ops agent on an insurer's claims & telematics stream: FNOL, coverage, fraud scoring, payouts. Seven incident scenarios (outages, vendor failure, bad deploy, hailstorm surge, fraud ring) diagnosed with root cause, business impact, owner and next steps — with PII redaction, insurance Cedar roles and an 18-slide deck. |

## Running a use case

Each use case runs from **its own folder** (it ships its own `mcp_streaming` package):

```bash
cd use-cases/auto-insurance
make install && source .venv/bin/activate   # its own virtualenv
make demo-offline                            # offline end-to-end demo (mock LLM)
make demo-scenario SCENARIO=staged_accident_ring
make test                                    # 29 tests
```

Already have the root virtualenv? Reuse it instead of creating a new one: `make demo-offline PYTHON=../../.venv/bin/python`.

See [`auto-insurance/README.md`](auto-insurance/README.md) for the full guide.

## How the folders are maintained

Each use case is developed on its own branch (`usecase/<name>`), which is the source of truth. The folder here is an exact copy of that branch. From the repository root, on `main`:

```bash
make check-use-cases     # ✓ in sync / ✗ lists differing files (exit 1 on drift)
make sync-use-cases      # refresh every folder from its branch, then review and commit
scripts/sync_use_cases.sh auto-insurance   # refresh just one
```

The script fetches `origin`, copies the branch's committed files with `git archive` (nothing untracked), and only touches `use-cases/<name>/`. It never commits or pushes.

Make changes on the branch first, then sync the folder, so the two never drift.

## Adding a new use case

1. Branch from `main`: `git checkout -b usecase/<name>`.
2. Add `mcp_streaming/domain/<name>.py` modelled on `auto_insurance.py` (services, event templates, scenarios/runbooks, roles, redaction), and point the simulator, agent prompts, mock provider and Cedar policies at it.
3. Update that branch's README, FAQ, tests and deck.
4. Copy it here with the `git archive` command above and add a row to the table.
