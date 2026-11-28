# Election lock model

The model covers startup acquisition and takeover through `internal/election.Lock`.
It includes refused attempts and their effect on the stored lock. The fence
can use a legacy committed revision or a WAL position ordered by term and
sequence. The caller supplies whether the previous lock has expired.

The default bounds are two nodes, two election terms and six representative
fences. Each edge is replayed in a fresh in-memory object store, using a
shortest path from the initial state to reach its source. The test checks
the decision, returned record and persisted record. The separate JSON
fixtures retain the two original revision-based regression cases.

The model abstracts the caller's lease-time decision, concurrent object-store
requests, WAL catch-up and leader lifecycle. The existing election and
integration suites cover other parts of that behavior.
