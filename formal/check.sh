#!/usr/bin/env bash
# Run TLC on one model config.
#   usage: check.sh <ConfigBasenameWithout.cfg> [pass|fail]
# `fail` asserts TLC finds an invariant violation (mutation configs).
# Requires tla2tools.jar next to this script, or $TLA2TOOLS_JAR.
set -euo pipefail
cd "$(dirname "$0")"

CFG="${1:?usage: check.sh <config-basename> [pass|fail]}"
EXPECT="${2:-pass}"
JAR="${TLA2TOOLS_JAR:-tla2tools.jar}"
OUT="$(mktemp)"
trap 'rm -f "$OUT"' EXIT

set +e
# -deadlock DISABLES deadlock checking: bounded models legitimately run out
# of enabled actions when MaxVersion/MaxEpoch/MaxCrashes are exhausted.
java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
  -workers auto -deadlock -cleanup \
  -config "${CFG}.cfg" DgenRegistryReplication.tla 2>&1 | tee "$OUT"
STATUS=${PIPESTATUS[0]}
set -e

if [ "$EXPECT" = "pass" ]; then
  exit "$STATUS"
fi

# expected-fail: demand a genuine invariant violation, so a parse error or
# JVM crash cannot masquerade as the expected counterexample.
if [ "$STATUS" -ne 0 ] && grep -Eq "Invariant .* is violated" "$OUT"; then
  echo "OK: expected invariant violation was found"
  exit 0
fi
echo "ERROR: expected an invariant violation, but TLC exited $STATUS without one"
exit 1
