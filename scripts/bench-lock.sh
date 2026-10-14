#!/usr/bin/env bash
# Run a benchmark while holding the machine-wide benchmark lock, so that
# measurements from concurrent checkouts/agents never overlap:
#
#   scripts/bench-lock.sh cargo run --release -p ant-bench -- fleet ...
#
# Builds and tests do not need the lock; only timed runs (fleet, soak, import,
# checkout, clone timings, scorecard) do. Waits for the lock, printing who
# holds it.
set -euo pipefail
dir="${ANT_SERVICES_DIR:-$HOME/.local/share/antimony-services}"
mkdir -p "$dir"
lock="$dir/bench.lock"
exec 9>>"$lock"
if ! flock -n 9; then
    echo "bench-lock: waiting for $(cat "$lock.owner" 2>/dev/null || echo 'another benchmark')" >&2
    flock 9
fi
echo "$(pwd) pid $$: $*" > "$lock.owner"
trap 'rm -f "$lock.owner"' EXIT
"$@"
