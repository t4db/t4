# Releasing T4

What a maintainer needs to know that is **not** already encoded in CI workflows or branch-protection settings. For everything else, treat those configs as the source of truth.

## What gates a release

Release tag (`v*`) push triggers `image-release.yml`, which gates publish on:

- `precheck` — `ci.yml` workflow must have completed with `success` on the release SHA (waits up to ~30 min).
- `nightly` — `nightly.yml` on the release SHA, minus the Kubernetes conformance suite: the full Jepsen nemesis × workload matrix on S3 and GCS, `TestLongRunningConsistency` (30 min stress) and etcd's three-node robustness tests. A test added to the nightly gates releases too, unless it is excluded here.

The full conformance suite is deliberately not on this path. In the most recent nightly it took 115 minutes, against 31 for the next longest job (the stress test) and 5 for the robustness tests, with the Jepsen cells running about 4 each in parallel — so it sets the release's wall clock by itself. Without it the gate finishes in roughly half an hour instead of two hours. It is also one long job on a single hosted runner, which is how the `v1.2.0` tag lost 1.5 h to `The hosted runner lost communication with the server` and had its release blocked for a reason unrelated to the code. That is cost and exposure rather than flakiness: conformance had not failed a nightly in the preceding two weeks, while the robustness tests failed three and Jepsen one.

Kubernetes coverage does not leave the release path with it. `ci.yml` runs Sonobuoy's quick mode on every PR, and so on the release SHA through `precheck`, which is what catches a T4 change that breaks Kubernetes; the full suite keeps running every night on `main`. What a release gives up is the long tail of the full suite on the exact release commit, which the nightly covers a few hours either side of it.

To run it against a release commit anyway, dispatch `nightly.yml` on the tag before or after cutting the release:

```bash
gh workflow run nightly.yml --ref vX.Y.Z
```

Required CI checks on `main` are whichever checks are marked **required** in branch-protection settings. That config is authoritative — this page does not duplicate the list.

## Cutting the release

1. Audit the v1 [compatibility contract](v1-compatibility.md) against the previous tag: `t4.Config` defaults, exported types, object-store layout, WAL frames. Any break needs a major version bump.
2. Tag and push:
   ```bash
   git tag -a vX.Y.Z -m "vX.Y.Z" <release-sha>
   git push origin vX.Y.Z
   ```
3. `image-release.yml` fires: precheck → nightly tests → draft release (notes auto-generated from merged PRs via `gh release create --generate-notes`) → multi-arch image to GHCR (cosign-signed, SLSA provenance attested) → six binaries: Linux, macOS and Windows on amd64 and arm64 (provenance attested) → `checksums.txt` (cosign-signed `.sig` + `.pem`).
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
