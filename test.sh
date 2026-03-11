#!/usr/bin/env bash
# test.sh — Run all CDC module test cases with pytest.
#
# Usage:
#   ./test.sh            # run all tests
#   ./test.sh -k Nulls   # run a specific test class

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Resolve Java (required by PySpark) ───────────────────────────────────────
if [ -z "${JAVA_HOME:-}" ]; then
  if [ -d "/opt/homebrew/opt/openjdk@17" ]; then
    export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
  elif [ -d "/opt/homebrew/opt/openjdk" ]; then
    export JAVA_HOME="/opt/homebrew/opt/openjdk"
  fi
fi
export PATH="${JAVA_HOME}/bin:${PATH}"

# ── Resolve Python (honours alias → explicit Homebrew path as fallback) ───────
PYTHON=""
for candidate in python3.11 python3 python; do
  full="$(command -v "$candidate" 2>/dev/null || true)"
  if [ -n "$full" ] && "$full" -c "import pyspark" &>/dev/null; then
    PYTHON="$full"
    break
  fi
done

if [ -z "$PYTHON" ]; then
  echo "ERROR: No Python with pyspark found." >&2
  echo "       Run:  pip install pyspark  (or activate your virtualenv first)" >&2
  exit 1
fi

if ! "$PYTHON" -c "import pytest" &>/dev/null; then
  echo "ERROR: pytest not installed. Run:  pip install pytest" >&2
  exit 1
fi

# ── Ensure Spark workers use the same Python as the driver ────────────────────
export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"

# ── Run tests ─────────────────────────────────────────────────────────────────
echo "Using Python: $PYTHON"
echo "Running CDC test suite..."
"$PYTHON" -m pytest tests/test_cdc.py -v "$@"
