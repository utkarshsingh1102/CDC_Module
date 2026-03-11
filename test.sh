#!/usr/bin/env bash
# test.sh — Run all CDC module test cases with pytest.
#
# Usage:
#   ./test.sh            # run all tests
#   ./test.sh -k Nulls   # run a specific test class

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Dependency check ──────────────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
  echo "ERROR: python3 not found. Please install Python 3.8+." >&2
  exit 1
fi

for pkg in pyspark pytest; do
  if ! python3 -c "import $pkg" &>/dev/null; then
    echo "ERROR: '$pkg' not installed. Run:  pip install $pkg" >&2
    exit 1
  fi
done

# ── Run tests ─────────────────────────────────────────────────────────────────
echo "Running CDC test suite..."
python3 -m pytest tests/test_cdc.py -v "$@"
