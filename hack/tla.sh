#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
jar="$("$repo_root/hack/download-tla.sh")"

(
  cd "$repo_root/specs/tla/election"
  java -Xmx1g -XX:+UseParallelGC -cp "$jar" tlc2.TLC \
    -deadlock -noGenerateSpecTE -workers 1 -metadir "$work/states" \
    -dump dot,actionlabels "$work/election.dot" -config Election.cfg Election.tla
)

cd "$repo_root"
T4_TLC_ELECTION_GRAPH="$work/election.dot" go test -count=1 -v ./tests/tla
