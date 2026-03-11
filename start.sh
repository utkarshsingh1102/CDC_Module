#!/usr/bin/env bash
# start.sh — Run the CDC pipeline.
#
# Usage:
#   ./start.sh                                        # built-in sample data
#   ./start.sh --prev path/to/prev.csv --curr path/to/curr.csv
#   ./start.sh --prev prev.csv --curr curr.csv --keys emp_id dept

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Dependency check ──────────────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
  echo "ERROR: python3 not found. Please install Python 3.8+." >&2
  exit 1
fi

if ! python3 -c "import pyspark" &>/dev/null; then
  echo "ERROR: pyspark not installed. Run:  pip install pyspark" >&2
  exit 1
fi

# ── Run ───────────────────────────────────────────────────────────────────────
echo "Starting CDC pipeline..."
python3 main.py "$@"
