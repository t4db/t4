---
title: Recipes
description: Common patterns for using T4 — distributed locks, service discovery, config management, and leader election.
---

## Multi-key atomic writes

Use `Txn` when you need to write several keys at once — either all succeed at the same revision or none do.

```go
// Write /account/balance and /account/last-updated atomically.
resp, err := node.Txn(ctx, t4.TxnRequest{
    Success: []t4.TxnOp{
        {Type: t4.TxnPut, Key: "/account/balance",      Value: []byte("1000")},
        {Type: t4.TxnPut, Key: "/account/last-updated", Value: []byte(time.Now().Format(time.RFC3339))},
    },
})
if err != nil {
    return err
}
// Both keys now share resp.Revision as their ModRevision.
```

---

## Conditional multi-key write (check-then-act)

`Txn` lets you guard a write on the current state of one or more keys. The classic pattern is "update a key only if it has not changed since I last read it":

```go
// Read the current state.
balanceKV, err := node.Get("/account/balance")
if err != nil || balanceKV == nil {
    return fmt.Errorf("account not found")
}
currentBalance, _ := strconv.ParseInt(string(balanceKV.Value), 10, 64)
currentRev := balanceKV.Revision

// Apply a debit only if no one else has modified /account/balance.
resp, err := node.Txn(ctx, t4.TxnRequest{
    Conditions: []t4.TxnCondition{
        {
            Key:         "/account/balance",
            Target:      t4.TxnCondMod,
            Result:      t4.TxnCondEqual,
            ModRevision: currentRev, // fail if another writer raced us
        },
    },
    Success: []t4.TxnOp{
        {Type: t4.TxnPut, Key: "/account/balance", Value: []byte(strconv.FormatInt(currentBalance-100, 10))},
        {Type: t4.TxnPut, Key: "/audit/last-debit", Value: []byte(time.Now().Format(time.RFC3339))},
    },
    // Failure branch is empty — condition failed, nothing written.
})
if err != nil {
    return err
}
if !resp.Succeeded {
    return fmt.Errorf("concurrent modification — retry")
}
```

### Create-if-absent across multiple keys

Use `TxnCondVersion == 0` to check that a key does not yet exist:

```go
resp, err := node.Txn(ctx, t4.TxnRequest{
    Conditions: []t4.TxnCondition{
        {Key: "/locks/job-42", Target: t4.TxnCondVersion, Result: t4.TxnCondEqual, Version: 0},
    },
    Success: []t4.TxnOp{
        {Type: t4.TxnPut, Key: "/locks/job-42",   Value: []byte("worker-1")},
        {Type: t4.TxnPut, Key: "/jobs/42/status",  Value: []byte("running")},
    },
})
if err != nil {
    return err
}
if !resp.Succeeded {
    return fmt.Errorf("job 42 is already locked")
}
```

### Atomic move (delete + put)

Move an item from one queue to another atomically — no other worker can claim it between the delete and the put:

```go
resp, err := node.Txn(ctx, t4.TxnRequest{
    Conditions: []t4.TxnCondition{
        // Source key must still be at the revision we read.
        {Key: "/queue/pending/task-7", Target: t4.TxnCondMod, Result: t4.TxnCondEqual, ModRevision: taskRev},
    },
    Success: []t4.TxnOp{
        {Type: t4.TxnDelete, Key: "/queue/pending/task-7"},
        {Type: t4.TxnPut,    Key: "/queue/processing/task-7", Value: taskData},
    },
})
if err != nil {
    return err
}
if !resp.Succeeded {
    return fmt.Errorf("task was already claimed by another worker")
}
_, wasDeleted := resp.DeletedKeys["/queue/pending/task-7"]
_ = wasDeleted // true when the source key existed and was removed
```

---

## Distributed lock

Use `Create` + `Delete` for a simple distributed lock. `Create` fails with `ErrKeyExists` if the key already exists, making it safe for concurrent acquisition.

```go
const lockKey = "/locks/my-resource"

func acquireLock(ctx context.Context, node *t4.Node, ttl time.Duration) (release func(), err error) {
    rev, err := node.Create(ctx, lockKey, []byte("locked"), 0)
    if err != nil {
        if errors.Is(err, t4.ErrKeyExists) {
            return nil, fmt.Errorf("lock already held")
        }
        return nil, err
    }

    release = func() {
        node.DeleteIfRevision(context.Background(), lockKey, rev)
    }
    return release, nil
}

// Usage
release, err := acquireLock(ctx, node, 30*time.Second)
if err != nil {
    log.Fatal("could not acquire lock:", err)
}
defer release()

// ... do work ...
```

### Lock with retry

```go
func acquireLockWithRetry(ctx context.Context, node *t4.Node, retryInterval time.Duration) (func(), error) {
    for {
        release, err := acquireLock(ctx, node, 0)
        if err == nil {
            return release, nil
        }
        if !errors.Is(err, t4.ErrKeyExists) {
            return nil, err
        }
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-time.After(retryInterval):
        }
    }
}
```

### Lock with watch (reactive wait)

Instead of polling, watch the lock key and retry only when it is released:

```go
func acquireLockWatching(ctx context.Context, node *t4.Node) (func(), error) {
    for {
        release, err := acquireLock(ctx, node, 0)
        if err == nil {
            return release, nil
        }
        if !errors.Is(err, t4.ErrKeyExists) {
            return nil, err
        }

        // Watch for the lock to be deleted, then retry.
        events, err := node.Watch(ctx, lockKey, 0)
        if err != nil {
            return nil, err
        }
        for e := range events {
            if e.Type == t4.EventDelete {
                break
            }
        }
        if ctx.Err() != nil {
            return nil, ctx.Err()
        }
    }
}
```

---

## Service discovery / registry

Store service endpoints under a common prefix. Each service writes its own key; clients list the prefix to discover all instances.

### Register

```go
const servicePrefix = "/services/my-api/"

func register(ctx context.Context, node *t4.Node, instanceID, addr string) error {
    key := servicePrefix + instanceID
    _, err := node.Put(ctx, key, []byte(addr), 0)
    return err
}

func deregister(ctx context.Context, node *t4.Node, instanceID string) error {
    _, err := node.Delete(ctx, servicePrefix+instanceID)
    return err
}
```

### Discover

```go
func discover(node *t4.Node) ([]string, error) {
    kvs, err := node.List(servicePrefix)
    if err != nil {
        return nil, err
    }
    addrs := make([]string, len(kvs))
    for i, kv := range kvs {
        addrs[i] = string(kv.Value)
    }
    return addrs, nil
}
```

### Watch for changes

```go
func watchRegistry(ctx context.Context, node *t4.Node, onChange func([]string)) error {
    events, err := node.Watch(ctx, servicePrefix, 0)
    if err != nil {
        return err
    }
    for range events {
        addrs, err := discover(node)
        if err != nil {
            continue
        }
        onChange(addrs)
    }
    return nil
}
```

---

## Configuration management

T4 is a natural fit for dynamic configuration — store config values as keys, watch for changes, and update in-process without a restart.

### Write config

```go
type AppConfig struct {
    Timeout  string `json:"timeout"`
    MaxConns int    `json:"max_conns"`
}

func writeConfig(ctx context.Context, node *t4.Node, cfg AppConfig) error {
    b, err := json.Marshal(cfg)
    if err != nil {
        return err
    }
    _, err = node.Put(ctx, "/config/app", b, 0)
    return err
}
```

### Read config

```go
func readConfig(node *t4.Node) (*AppConfig, error) {
    kv, err := node.Get("/config/app")
    if err != nil {
        return nil, err
    }
    if kv == nil {
        return nil, fmt.Errorf("config not found")
    }
    var cfg AppConfig
    return &cfg, json.Unmarshal(kv.Value, &cfg)
}
```

### Hot-reload on change

```go
func watchConfig(ctx context.Context, node *t4.Node, apply func(AppConfig)) {
    events, _ := node.Watch(ctx, "/config/app", 0)
    for e := range events {
        if e.Type != t4.EventPut {
            continue
        }
        var cfg AppConfig
        if err := json.Unmarshal(e.KV.Value, &cfg); err == nil {
            apply(cfg)
        }
    }
}

// In main:
go watchConfig(ctx, node, func(cfg AppConfig) {
    log.Printf("config updated: timeout=%s max_conns=%d", cfg.Timeout, cfg.MaxConns)
    applyConfig(cfg)
})
```

---

## Compare-and-swap (CAS) counter

Use `Update` to increment a counter atomically. `Update` is a compare-and-swap on the key's modification revision — it succeeds only if the revision you read is still current.

```go
func increment(ctx context.Context, node *t4.Node, key string) (int64, error) {
    for {
        kv, err := node.Get(key)
        if err != nil {
            return 0, err
        }

        var current int64
        var prevRev int64
        if kv != nil {
            current, _ = strconv.ParseInt(string(kv.Value), 10, 64)
            prevRev = kv.Revision
        }

        next := current + 1
        _, _, ok, err := node.Update(ctx, key, []byte(strconv.FormatInt(next, 10)), prevRev, 0)
        if err != nil {
            return 0, err
        }
        if ok {
            return next, nil
        }
        // Revision changed — someone else updated; retry.
    }
}
```

---

## Leader election in your application

Use T4's `Create` + `Watch` to implement leader election for your own application on top of T4.

```go
const electionKey = "/election/my-service"

type Election struct {
    node     *t4.Node
    id       string
    isLeader atomic.Bool
}

func (e *Election) Run(ctx context.Context) error {
    for {
        // Try to become leader.
        rev, err := e.node.Create(ctx, electionKey, []byte(e.id), 0)
        if err == nil {
            // Won the election.
            e.isLeader.Store(true)
            log.Printf("%s: became leader at rev %d", e.id, rev)
            e.runAsLeader(ctx)
            e.isLeader.Store(false)
            continue
        }
        if !errors.Is(err, t4.ErrKeyExists) {
            return err
        }

        // Someone else is leader — watch until they step down.
        log.Printf("%s: following", e.id)
        events, err := e.node.Watch(ctx, electionKey, 0)
        if err != nil {
            return err
        }
        for event := range events {
            if event.Type == t4.EventDelete {
                break // Leader stepped down — try to acquire.
            }
        }
        if ctx.Err() != nil {
            return ctx.Err()
        }
    }
}

func (e *Election) runAsLeader(ctx context.Context) {
    defer e.node.Delete(ctx, electionKey) // Resign on exit.
    // Do leader work until ctx is cancelled or an error occurs.
    <-ctx.Done()
}
```

---

## Consistent read after write

After a follower serves a write that was forwarded to the leader, you may want to guarantee a subsequent read reflects that write. Use `WaitForRevision`:

```go
rev, err := node.Put(ctx, "/data/key", value, 0)
if err != nil {
    return err
}

// Ensure the local node has applied this revision before reading.
if err := node.WaitForRevision(ctx, rev); err != nil {
    return err
}

kv, err := node.Get("/data/key")
```

Or use `LinearizableGet` to sync to the leader's current revision in one call:

```go
kv, err := node.LinearizableGet(ctx, "/data/key")
```

---

## History compaction

T4 keeps the full revision history until you compact. Compact periodically to bound storage growth:

```go
func compactPeriodically(ctx context.Context, node *t4.Node, interval time.Duration) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            rev := node.CurrentRevision()
            if err := node.Compact(ctx, rev); err != nil {
                log.Printf("compact error: %v", err)
            }
        }
    }
}
```
