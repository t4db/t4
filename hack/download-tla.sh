#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version=1.8.0
sha256=fdffef247f563ef6edbfd44958d2a82db79e7564e07cb673262d896353fa6713
jar_dir="${TLA_TOOLS_DIR:-$repo_root/.tmp/tla}"
jar_path="${TLA2TOOLS_JAR:-$jar_dir/tla2tools-$version.jar}"

verify() {
  local got
  got="$(shasum -a 256 "$1" | awk '{print $1}')"
  [[ "$got" == "$sha256" ]]
}

if [[ -n "${TLA2TOOLS_JAR:-}" ]]; then
  if [[ ! -f "$jar_path" ]] || ! verify "$jar_path"; then
    echo 'TLA2TOOLS_JAR must point to the pinned TLC jar' >&2
    exit 1
  fi
elif [[ ! -f "$jar_path" ]] || ! verify "$jar_path"; then
  mkdir -p "$jar_dir"
  temp="$(mktemp "$jar_dir/tla2tools.XXXXXX")"
  trap 'rm -f "$temp"' EXIT
  curl --retry 3 -fsSL -o "$temp" \
    "https://github.com/tlaplus/tlaplus/releases/download/v$version/tla2tools.jar"
  if ! verify "$temp"; then
    echo 'Downloaded TLC jar failed SHA-256 verification' >&2
    exit 1
  fi
  mv "$temp" "$jar_path"
fi

printf '%s\n' "$(cd "$(dirname "$jar_path")" && pwd)/$(basename "$jar_path")"
