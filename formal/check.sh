#!/usr/bin/env bash
# Run TLC on one model config.
#   usage: check.sh <ConfigBasenameWithout.cfg> [pass|fail]
# `fail` asserts TLC finds an invariant violation (mutation configs).
# Requires tla2tools.jar next to this script, or $TLA2TOOLS_JAR.
set -euo pipefail
cd "$(dirname "$0")"

CFG="${1:?usage: check.sh <config-basename> [pass|fail]}"
EXPECT="${2:-pass}"
# Jar resolution, most specific first:
#   1. $TLA2TOOLS_JAR — an explicit override.
#   2. formal/tla2tools.jar — a personal local jar (gitignored; e.g. a newer TLC
#      build for faster local runs).
#   3. formal/vendor/tla2tools-v1.8.0.jar — the CHECKED-IN jar, which CI and a
#      fresh clone use. Vendored because GitHub release assets on the tlaplus
#      tag are republished in place: the v1.8.0 asset's sha256 changed under our
#      pin twice, breaking CI each time. A jar in git is immutable, hermetic
#      (no download step to flake), and reviewed once.
if [ -n "${TLA2TOOLS_JAR:-}" ]; then
  JAR="$TLA2TOOLS_JAR"
elif [ -f tla2tools.jar ]; then
  JAR="tla2tools.jar"
else
  JAR="vendor/tla2tools-v1.8.0.jar"
fi
OUT="$(mktemp)"
# A private scratch metadir per run. TLC's default (states/ shared by every
# invocation) plus -cleanup lets one concurrent run delete another's live state
# pool mid-write — observed as "Error: when writing the disk (StatePoolWriter)"
# killing the main-config run whenever two check.sh ran at once. mktemp -d makes
# concurrent invocations (a make -j, two terminals, parallel CI steps) safe;
# the trap replaces -cleanup.
META="$(mktemp -d)"
trap 'rm -rf "$OUT" "$META"' EXIT

set +e
# -deadlock DISABLES deadlock checking: bounded models legitimately run out
# of enabled actions when MaxVersion/MaxEpoch/MaxCrashes are exhausted.
java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
  -workers auto -deadlock -metadir "$META" \
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
