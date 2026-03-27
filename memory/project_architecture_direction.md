---
name: Strata architecture direction
description: Kine is a temporary client adapter; strata is designed to be both embeddable and standalone; kine backend will eventually live in the kine repo
type: project
---

Strata should support two usage modes: embedded (library via `strata.Open(cfg)`) and standalone (CLI via `cmd/strata/main.go`).

**Why:** The user explicitly said "kine is just one of the clients and probably will be in the kine repo. eventually we will have this db both embedded and standalone."

**How to apply:**
- Keep the public API (`strata.Node`, `strata.Config`, `strata.KeyValue`, etc.) stable and clean — it's the embedding surface.
- The `kine/` package is a thin adapter only; do not add core logic there.
- Do not couple `cmd/strata/main.go` to kine — the binary should be agnostic to which client adapter is used.
- When designing new features (metrics, health, TLS), make them configurable at the `Config` or `Node` level so embedded users can use them too, not just the CLI.
