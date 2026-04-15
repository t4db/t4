---
title: Branches
---

# Branching in t4

t4 supports **instant, zero-copy database branching**.

A branch is a fully independent database that starts from a consistent snapshot and evolves on its own — without duplicating existing data.

---

## Overview

Branching allows you to:

- Fork a database at any checkpoint
- Run isolated experiments safely
- Create multiple independent timelines
- Avoid copying large datasets

All branches share immutable data and only write their own changes.

---

## How it works

### 1. Branches start from a checkpoint

A branch is created from a **checkpoint**, which represents a consistent snapshot of the database.

Internally, a branch stores:
- a reference to the checkpoint
- its own identity (branch ID)

At creation time, no data is copied.

---

### 2. Data is shared via content-addressed storage

t4 stores data in immutable SST files identified by hash.

When a branch is created:
- existing SST files are reused
- no duplication occurs
- the branch reads from shared storage

This is effectively **copy-on-write at the storage layer**.

---

### 3. Writes are isolated per branch

After creation:

- each branch maintains its own WAL
- new data is written to the branch’s own S3 prefix
- existing data remains shared

So each branch evolves independently.

---

### 4. Branches are first-class databases

A branch behaves like a normal database:

- accepts reads and writes
- produces its own checkpoints
- maintains its own history

There is no difference between a “main” database and a branch.

---

### 5. Garbage collection is branch-aware

Because data is shared:

- SST files referenced by any branch are preserved
- the source database cannot delete data still in use

When a branch is removed:
- its references are released
- unused data becomes eligible for garbage collection

---

## Key properties

### Instant creation

Creating a branch is an **O(1) operation**:
- no data copying
- only metadata is written

---

### Zero-copy

Branches reuse existing data:
- no duplication of SST files
- minimal storage overhead

---

### Strong consistency

Branches start from a checkpoint:
- no partial state
- no race conditions
- fully deterministic starting point

---

### Independent evolution

Each branch:
- has its own WAL
- writes its own data
- can diverge arbitrarily

---

### Storage-efficient isolation

You get isolation without the cost of copying entire datasets.

---

## Mental model

You can think of t4 branching as:

| Concept | t4 |
|--------|----|
| Snapshot | Checkpoint |
| Branch | Branch |
| Immutable objects | SST files |
| History | WAL |
| Copy-on-write | Shared SSTs |

t4 behaves similarly to a versioned storage system, but for a key-value database.

---

## What branching is not

- Not MVCC snapshots (these are read-time views, not independent databases)
- Not logical namespaces (data is not duplicated via key prefixes)
- Not full copies (no cloning of entire datasets)

Branching happens at the **storage level**, not the application level.

---

## Use cases

### Safe experimentation

Try changes without affecting the main database:
- test new logic
- run migrations
- validate assumptions

---

### Parallel workflows

Run multiple independent flows:
- A/B testing
- alternative strategies
- concurrent processing paths

---

### Reproducibility

Recreate exact past states:
- debug issues
- replay scenarios
- verify behavior deterministically

---

### Isolation without cost

Avoid expensive cloning:
- no large data copies
- minimal additional storage usage

---

## Summary

t4 branching provides:

- instant database forks
- zero-copy data sharing
- independent evolution of branches
- storage-efficient isolation

All built on top of immutable SST files, WAL history, and S3-backed durability.

---

## One-line explanation

**Branching in t4 lets you fork a database instantly and evolve it independently, without copying data.**