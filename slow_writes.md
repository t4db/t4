  ---
Write path, exactly

Every Put (on a leader or single-node) goes through this sequence, all serialized under a single n.mu lock:

Put()
n.mu.Lock()
db.Get(key)                          ← Pebble point read (create vs update)
appendAndApply(entry):
wal.Append():
wal.mu.Lock()
marshalEntry()                   ← alloc #1: 49+len(key)+len(value) bytes
AppendEntry() → make(frame)      ← alloc #2: 8+len(payload) bytes
f.Write(frame)                   ← write to OS page cache
f.Sync()                         ← fsync #1 ← ~8-15 ms on NVMe
db.Apply([entry]):
pebble.NewBatch()
db.Get(logKey(rev))              ← Pebble bloom-filter + read (term-conflict check)
b.Set(logKey, marshalRecord())   ← alloc #3: 29+key+value bytes
b.Set(idxKey, encodeRev())       ← alloc #4: 8 bytes
b.Commit(pebble.Sync)            ← fsync #2 ← ~8-15 ms on NVMe
broadcast()                      ← close+create notify channel
peerSrv.Broadcast()                ← non-blocking channel send
n.mu.Unlock()

The benchmark shows ~15–20 ms per write = ~50–65 writes/second. That is exactly what two sequential fsyncs on an NVMe drive cost.

  ---
What makes it slow

1. Two fsyncs per write (dominant, ~15–20 ms total)

wal.Append calls f.Sync() per entry, then db.Apply calls b.Commit(pebble.Sync). Both block until the OS confirms the data is on
physical media.

This is the correct durability story, but doing them sequentially, once per write is the worst possible usage pattern.

2. Global mutex holds across both fsyncs

n.mu is held from before the Pebble read all the way through both fsyncs and the Pebble commit. While one Put is blocked on disk,
every other writer in the process is queued behind it. There is zero write concurrency.

3. WAL entry is serialized into two separate heap allocations

marshalEntry allocates the payload buffer, then AppendEntry wraps it in a new frame buffer. There's also a bytes.Buffer allocated in
SegmentWriter.Append just to pass to AppendEntry. Three allocations and two copies to write ~70 bytes.

4. One-entry Pebble batch per write

db.Apply creates a pebble.Batch, writes two keys into it (log + index), commits with sync, and closes it. Pebble's batch machinery
has setup cost, and pebble.Sync tells Pebble to flush and fsync — same cost as if you'd called db.Set directly. The batch abstraction
buys nothing when it always contains exactly one entry.

5. Pebble read inside every write

applyEntry does s.db.Get(logKey(rev)) on the write path to clean up stale index entries after a term change. The code comments say
"no-op on the normal write path thanks to Bloom filter short-circuit" but it still goes through the Bloom filter lookup and the lock
on every write.

6. Pebble read before WAL append

putLocked calls db.Get(key) to decide OpCreate vs OpUpdate. This is a legitimate read (needed for CreateRevision and PrevRevision),
but it holds n.mu while waiting on Pebble.

  ---
How to make it faster

Option A: Group-commit the WAL (highest impact, ~10×)

Instead of fsyncing once per entry, collect writes from multiple callers and fsync them together. This is the same technique used by
PostgreSQL, etcd, and every high-throughput database.

Concretely: callers drop their serialized entry into a shared queue and block on a per-entry response channel. A single background
goroutine drains the queue, writes all pending entries in one writev/Write, calls f.Sync() once for the whole batch, then unblocks
all waiters. The WAL fsync cost stays ~8–15 ms but is now amortized across all writes that arrived during that window. At 1000
concurrent writers you still pay one fsync; at 10 writers you pay one fsync. This completely replaces the current per-entry f.Sync()
with no durability loss (all entries are fsynced before any caller returns).

The tricky part is that the current WAL lock (wal.mu) must be released while waiting so other callers can queue, and n.mu must also
not be held across the WAL wait — see option C below.

Option B: Eliminate the second fsync by making Pebble write-behind

Pebble does not need to fsync on every commit. Its WAL is now redundant — the strata WAL is the source of truth and Pebble is a
queryable index that can be rebuilt. b.Commit(pebble.NoSync) is safe: if the process crashes after the strata WAL fsync but before
Pebble flushes, the next startup runs replayLocal/replayRemote and rebuilds Pebble state. Switching from pebble.Sync to pebble.NoSync
eliminates fsync #2 entirely, cutting write latency roughly in half.

This applies to both Apply and Recover.

Option C: Release n.mu between WAL write and Pebble apply

After the WAL fsync, the entry is durable. The Pebble apply is just an index update and can happen while other writes are queued. If
n.mu is released before the Pebble b.Commit, and re-acquired only to advance currentRev, then multiple WAL writes can pipeline with
Pebble applies. This requires careful revision ordering (entries need to be applied to Pebble in sequence even if WAL writes are
batched out of order).

A simpler split: release n.mu after the WAL append and re-acquire it only to call db.Apply. The lock is still held for Pebble
reads/writes but not across the fsync stall.

Option D: Eliminate the Pebble-Get on every write

The s.db.Get(logKey(rev)) in applyEntry is only needed during WAL replay to handle the case where a new leader writes a different key
at the same revision as the old leader. On the live write path (not replay), a revision is always new — it can never exist in the DB
yet. Moving this check to Recover only (where it is actually needed) eliminates one Bloom filter lookup per write.

Option E: Single-allocation WAL encoding

Pre-allocate one buffer of size 8 + entryFixedSize + len(key) + len(value) and write the frame header, payload, and CRC into it
directly, without the intermediate marshalEntry allocation and the bytes.Buffer. This won't change latency meaningfully (allocations
are fast) but reduces GC pressure at high write rates.

  ---
Summary table

┌──────────────────────────────┬─────────────────────┬────────────┐
│            Option            │  Latency reduction  │ Complexity │
├──────────────────────────────┼─────────────────────┼────────────┤
│ A: Group-commit WAL          │ ~10× at concurrency │ High       │
├──────────────────────────────┼─────────────────────┼────────────┤
│ B: Pebble NoSync             │ ~2× unconditionally │ Very low   │
├──────────────────────────────┼─────────────────────┼────────────┤
│ C: Release lock across fsync │ ~2× at concurrency  │ Medium     │
├──────────────────────────────┼─────────────────────┼────────────┤
│ D: Skip Pebble-Get in Apply  │ ~5%                 │ Low        │
├──────────────────────────────┼─────────────────────┼────────────┤
│ E: Single-alloc encoding     │ ~1% (GC)            │ Low        │
└──────────────────────────────┴─────────────────────┴────────────┘

B is the obvious first step — one-line change, safe, halves latency immediately. A is the right long-term fix for any workload with
more than one concurrent writer. They compose: with both B and A, write throughput goes from ~60/s to potentially thousands/s on the
same hardware.

✻ Cogitated for 4m 4s

⏺ Background command "Run Put benchmark to get baseline numbers" failed with exit code 1

⏺ The benchmark results I already captured from the separate run are the relevant numbers (~15-20 ms/op). The background task that
failed was an earlier run that got mixed in with test output — not needed.