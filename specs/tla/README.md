# TLA+ models

Run the election model and Go replay together with `make tla`. The target
requires Java 21 and the Go version in `go.mod`. It downloads and verifies the
pinned TLC jar, or verifies a local copy supplied through `TLA2TOOLS_JAR`.

TLC checks the [election model](election/README.md) and exports a fresh state
graph to a temporary directory. The Go test replays each graph edge through
the production lock API. CI runs the same target.
