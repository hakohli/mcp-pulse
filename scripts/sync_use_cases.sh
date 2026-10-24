#!/usr/bin/env bash
# Keep use-cases/<name>/ on main identical to its source-of-truth branch usecase/<name>.
#
#   scripts/sync_use_cases.sh                 # refresh every use case from its branch
#   scripts/sync_use_cases.sh auto-insurance  # refresh one
#   scripts/sync_use_cases.sh --check         # report drift only; exit 1 if any folder differs
#
# Uses the remote branch (origin/usecase/<name>) after a fetch, falling back to the
# local branch. Only touches files under use-cases/<name>/ — review and commit the
# result yourself; nothing is committed or pushed.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

check=0
names=()
for arg in "$@"; do
  case "$arg" in
    --check) check=1 ;;
    -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
    *) names+=("$arg") ;;
  esac
done

if [ ${#names[@]} -eq 0 ]; then
  for d in use-cases/*/; do names+=("$(basename "$d")"); done
fi

git fetch -q origin 2>/dev/null || echo "warning: git fetch failed; using local branches" >&2

drift=0
for name in "${names[@]}"; do
  ref="origin/usecase/$name"
  git rev-parse -q --verify "$ref" >/dev/null || ref="usecase/$name"
  if ! git rev-parse -q --verify "$ref" >/dev/null; then
    echo "✗ $name: no branch usecase/$name found" >&2
    drift=1
    continue
  fi

  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  git archive "$ref" | tar -x -C "$tmp"
  short="$(git rev-parse --short "$ref")"

  if diff -rq "$tmp" "use-cases/$name" >/dev/null 2>&1; then
    echo "✓ $name: in sync with $ref ($short)"
  elif [ "$check" -eq 1 ]; then
    echo "✗ $name: differs from $ref ($short):"
    diff -rq "$tmp" "use-cases/$name" | sed 's/^/    /' || true
    drift=1
  else
    rm -rf "use-cases/$name"
    mkdir -p "use-cases/$name"
    cp -R "$tmp/." "use-cases/$name/"
    echo "↻ $name: refreshed from $ref ($short) — review with: git status use-cases/$name"
  fi
  rm -rf "$tmp"
  trap - EXIT
done

exit "$drift"
