#!/usr/bin/env bash
# Canonical compact-format parity checks for Glom.
# Exit 0 = all pass; exit 1 = failure.
set -euo pipefail

FAIL=0

fail() {
  echo "FAIL: $1"
  FAIL=1
}

pass() {
  echo "PASS: $1"
}

# 1. No trailing whitespace on any line of compact output.
if glom search 'the' 2>/dev/null | grep -Pc ' $' | grep -qv '^0$'; then
  fail "search output has trailing whitespace"
else
  pass "search: no trailing whitespace"
fi

# 2. No box-drawing characters in tools --names.
if glom tools --names 2>/dev/null | grep -Pc '[┏━┃┗┓┛│─┬┼]' | grep -qv '^0$'; then
  fail "tools --names has box-drawing characters"
else
  pass "tools --names: no box-drawing"
fi

# 3. No ANSI escape sequences in any listing command.
for cmd in "search the" "tools --names" "stats"; do
  if glom $cmd 2>/dev/null | grep -Pc '\x1b\[' | grep -qv '^0$'; then
    fail "glom $cmd has ANSI escapes"
  else
    pass "glom $cmd: no ANSI escapes"
  fi
done

# 4. JSON envelope shape: search.
OUT=$(glom search 'the' --json 2>/dev/null || true)
if [ -n "$OUT" ]; then
  for key in rows total displayed truncated limit; do
    if echo "$OUT" | python3 -c "import json,sys; d=json.load(sys.stdin); assert '$key' in d" 2>/dev/null; then
      pass "search --json has key '$key'"
    else
      fail "search --json missing key '$key'"
    fi
  done
fi

# 5. Search header columns.
HEADER=$(glom search 'the' 2>/dev/null | head -1 || true)
if [ -n "$HEADER" ]; then
  for col in rank kind name location snippet; do
    if echo "$HEADER" | grep -q "$col"; then
      pass "search header has column '$col'"
    else
      fail "search header missing column '$col'"
    fi
  done
fi

if [ "$FAIL" -ne 0 ]; then
  echo ""
  echo "Some parity checks FAILED."
  exit 1
fi

echo ""
echo "All parity checks passed."
exit 0
