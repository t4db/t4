# Releasing T4

What a maintainer needs to know that is **not** already encoded in CI workflows or branch-protection settings. For everything else, treat those configs as the source of truth.

## What gates a release

Release tag (`v*`) push triggers `image-release.yml`, which gates publish on:

- `precheck` — `ci.yml` workflow must have completed with `success` on the release SHA (waits up to ~30 min).
- `jepsen` — full nemesis matrix (`partition-halves`, `kill`, `partition-s3`) on the release SHA via the reusable `jepsen-workflow.yml`. Guards linearizability per-release, not just nightly.

`nightly.yml` still runs the same jepsen matrix plus `TestLongRunningConsistency` (30 min stress). Stress is not gated on release; if you care about it for a specific tag, trigger nightly on the SHA first:

```bash
gh workflow run nightly.yml --ref <release-sha>
```

Required CI checks on `main` are whichever checks are marked **required** in branch-protection settings. That config is authoritative — this page does not duplicate the list.

## Cutting the release

1. Audit the v1 [compatibility contract](v1-compatibility.md) against the previous tag: `t4.Config` defaults, exported types, object-store layout, WAL frames. Any break needs a major version bump.
2. Tag and push:
   ```bash
   git tag -a vX.Y.Z -m "vX.Y.Z" <release-sha>
   git push origin vX.Y.Z
   ```
3. `image-release.yml` fires: precheck → jepsen → draft release (notes auto-generated from merged PRs via `gh release create --generate-notes`) → multi-arch image to GHCR (cosign-signed, SLSA provenance attested) → four binaries (provenance attested) → `checksums.txt` (cosign-signed `.sig` + `.pem`).
4. Review the draft release notes, edit if needed, then unpublish "Draft".

## Verifying release artifacts

Image:

```bash
cosign verify ghcr.io/<owner>/t4:X.Y.Z \
  --certificate-identity-regexp 'https://github\.com/<owner>/t4/\.github/workflows/image-release\.yml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Checksums:

```bash
cosign verify-blob checksums.txt \
  --signature checksums.txt.sig \
  --certificate checksums.txt.pem \
  --certificate-identity-regexp 'https://github\.com/<owner>/t4/\.github/workflows/image-release\.yml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

SLSA provenance attestations (image and binaries) are queryable via `gh attestation verify`.

The Helm chart is released on a separate `chart/vA.B.C` tag (`chart-release.yml`). Only push it when the chart actually changed since the last chart tag. Don't tag the chart on prerelease (`v*-rc*`) SHAs — `chart-release.yml` picks the most recent `v*` tag for `appVersion`, which would stamp the prerelease into the chart.

## Rollback policy

If a regression slips out, **move forward, never rewrite**. Cut `vX.Y.(Z+1)` from a clean commit on `main`. Do not force-push or delete a published tag — downstream tooling caches resolved versions.

Mark the broken release as pre-release in the GitHub UI if it must be hidden.
