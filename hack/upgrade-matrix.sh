#!/usr/bin/env bash
# Prints the releases the upgrade tests start from, as a JSON array:
#
#   - the latest patch of the previous minor release,
#   - the first patch of the latest minor release,
#   - the latest release.
#
# For a latest release of v1.1.7 that is ["v1.0.6","v1.1.0","v1.1.7"].
#
# Releases are read from GitHub (drafts and pre-releases excluded), since the
# tests download their binaries. Pass tags as arguments to skip the lookup.
set -euo pipefail

if [[ $# -gt 0 ]]; then
  tags=$(printf '%s\n' "$@")
else
  tags=$(gh release list --limit 1000 --exclude-drafts --exclude-pre-releases \
    --json tagName --jq '.[].tagName')
fi
tags=$(grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' <<<"$tags" | sort -V || true)
if [[ -z "$tags" ]]; then
  echo "upgrade-matrix: no releases found" >&2
  exit 1
fi

latest=$(tail -n1 <<<"$tags")
minor=${latest%.*} # v1.1
first=$(grep -E "^${minor//./\\.}\.[0-9]+$" <<<"$tags" | head -n1)
prev=$(grep -vE "^${minor//./\\.}\.[0-9]+$" <<<"$tags" | tail -n1 || true)

printf '%s\n' "$prev" "$first" "$latest" | awk 'NF && !seen[$0]++' |
  jq -R . | jq -cs .
