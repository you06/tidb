# WORK03: Add lease-based cross-node invalidation via client-go precommit hook

## Description

Add lease-based cross-node invalidation to the transaction commit path. When a cached table is modified, a **precommit hook** in client-go fires after prewrite succeeds and `commitTS` is determined. The hook writes per-key invalidation entries (WORK01) and waits one lease before returning, allowing the commit to proceed.

This eliminates the need for a manual prewrite/commit split or retry loop — the 2PC flow in client-go orchestrates the phases, and the hook runs at exactly the right point.

## Client-go changes required

### Add `precommitHook` to `KVTxn`

File: `txnkv/transaction/txn.go`

Add a new field to `KVTxn`:

```go
// precommitHook is called after prewrite succeeds and commitTS is determined,
// but before the actual commit phase. This enables callers (e.g. TiDB cached
// table invalidation) to perform work that depends on the known commitTS
// before the data becomes visible. When set, async commit and 1PC are disabled
// to ensure commitTS is deterministic and known before commit.
precommitHook func(ctx context.Context, commitTS uint64) error
```

Add setter (following existing pattern like `SetCommitTSUpperBoundCheck`):

```go
// SetPrecommitHook sets a hook that is called after prewrite and commitTS
// determination, but before the commit phase. When set, 1PC and async commit
// are disabled to guarantee a deterministic commitTS is available.
func (txn *KVTxn) SetPrecommitHook(f func(ctx context.Context, commitTS uint64) error) {
    txn.precommitHook = f
}
```

### Disable async commit and 1PC when hook is set

File: `txnkv/transaction/2pc.go`

In `checkAsyncCommit()` (~line 1555), add alongside the existing `commitTSUpperBoundCheck` guard:

```go
if c.txn.precommitHook != nil {
    return false
}
```

In `checkOnePC()` (~line 1584), same pattern:

```go
if c.txn.precommitHook != nil {
    return false
}
```

### Call the hook in `execute()` after commitTS is determined

File: `txnkv/transaction/2pc.go`

Insert after the `commitTSUpperBoundCheck` block (~line 1963), before the `beforeCommit` failpoint:

```go
if c.txn.precommitHook != nil {
    if err = c.txn.precommitHook(ctx, commitTS); err != nil {
        return err
    }
}
```

At this point in `execute()`:
- Prewrite has succeeded (line 1841)
- `commitTS` has been fetched from TSO (line 1931) and stored (line 1950)
- Schema validity and expiry checks have passed (lines 1944-1956)
- `commitTSUpperBoundCheck` has passed (lines 1957-1963)
- Async commit and 1PC are disabled, so the code will proceed to `commitTxn` (line 2025)

If the hook returns an error, the commit aborts (prewrite locks will be cleaned up by TiKV's lock resolver after TTL expires).

## TiDB-side integration: session commit path

### Files to modify

- `pkg/session/session.go` (~line 568-579): Before the existing commit logic, set up the precommit hook for cached table transactions.
- `pkg/sessionctx/variable/session.go` (line 197): Change `CachedTables map[int64]any` to `CachedTables map[int64]struct{}`.
- `pkg/table/tblctx/table.go` (line 44-50): Simplify `CachedTableSupport` interface — `AddCachedTableHandleToTxn` becomes `MarkCachedTableModified(tableID int64)`.
- `pkg/table/tblsession/table.go` (line 137-154): Update implementation.
- `pkg/table/tables/cache.go` (lines 242-270): Update `AddRecord`/`UpdateRecord`/`RemoveRecord` to call new interface.
- New or existing file for the background invalidation poller (e.g. `pkg/domain/cachetable_invalidation.go`): Implement the per-node background goroutine that polls `mysql.table_cache_invalidation` (WORK01).

### Approach for commit path

```go
const cachedTableLease = 100 * time.Millisecond

// In doCommit, before existing cached table logic:
if tables := sessVars.TxnCtx.CachedTables; len(tables) > 0 {
    // Collect modified keys belonging to cached tables from the transaction's mutation buffer.
    modifiedKeys := collectCachedTableKeys(s.txn, tables)

    // Set the precommit hook. This fires after prewrite succeeds and commitTS is known.
    s.txn.SetPrecommitHook(func(ctx context.Context, commitTS uint64) error {
        // 1. Write per-key invalidation entries to mysql.table_cache_invalidation
        //    in a SEPARATE internal transaction (WORK01).
        //    Each entry records (tid, cache_key, min_cached_ts = commitTS).
        if err := writeInvalidationEntries(ctx, s, modifiedKeys, commitTS); err != nil {
            return err
        }
        // 2. Wait one lease so all nodes' pollers see the invalidation
        //    and evict/block cached entries before the commit becomes visible.
        time.Sleep(cachedTableLease)
        return nil
    })
}
```

No retry loop is needed. The hook runs exactly once with the final `commitTS`. Since async commit and 1PC are disabled when the hook is set, `commitTS` is deterministic (fetched from TSO after prewrite).

## Cross-node linearizability via lease

The lease duration is a const (e.g. 100ms).

### Write path (with precommit hook)

1. Transaction prewrites normally (2PC phase 1). Locks are placed on modified keys.
2. `commitTS` is fetched from TSO.
3. **Precommit hook fires**:
   a. Writes per-key invalidation entries to `mysql.table_cache_invalidation` with `min_cached_ts = commitTS` (WORK01). This is immediately visible to all nodes.
   b. Waits one full lease.
4. Commit (2PC phase 2). New data becomes visible at `commitTS`.

### Preventing re-caching during the invalidation window

When a node's poller sees invalidation entries for keys with `min_cached_ts`, it evicts those keys from freecache and records `min_cached_ts` per-key (WORK02). Subsequent reads with `readTS <= min_cached_ts` **must not re-cache** — they go directly to TiKV. Only when `readTS > min_cached_ts` can the node cache results again.

```
T=0       Prewrite succeeds. commitTS=80 determined.
T=0       Hook writes invalidation entries: min_cached_ts=80 for each modified key.
T=10      Node X polls, sees entries, evicts keys, records min_cached_ts=80 per-key.
T=20      Node X reads key (readTS=20). readTS=20 <= 80, does NOT cache. Reads from TiKV → old data (correct, commit hasn't happened yet).
T=100     Lease wait done. Hook returns. Commit proceeds. Data visible at commitTS=80.
T=110     Node X reads key (readTS=110). readTS=110 > 80, CAN cache. Fetches from TiKV → sees committed data. Caches it. Correct.
```

### Background poller (every TiDB node)

- Each TiDB node runs a background goroutine that polls `mysql.table_cache_invalidation` (WORK01) every **half-lease** (50ms).
- On seeing entries for keys with `min_cached_ts`, it calls `cacheDB.InvalidateKeys(keys, minCachedTS)` (WORK02) for each group.
- **Fallback**: if `current_time - last_poll_success_time >= lease` (100ms), the node **invalidates all cached data** unconditionally (clears freecache). The cache is refilled once polling catches back up.
- The poller also cleans up expired entries from the invalidation table (WORK01 cleanup strategy).

### Important

Keep the OLD `cachedTableRenewLease` logic in parallel temporarily (both old and new write paths active). The old path will be removed in WORK11.

## Testing

```bash
go test ./pkg/session/... --tags=intest
```

Focused on cached table commit tests.

## Important Reminders

- Read all relevant code before making changes. Understand existing patterns.
- Keep changes simple - no over-engineering, no unnecessary abstractions.
- Clean up tech debt you encounter, but don't change code logic during cleanup.
- For every added/modified function, add/modify corresponding tests.
- After each step, run the module's tests: `go test ./pkg/xxx/... --tags=intest` to ensure they pass.
- If a package uses failpoints, enable them before tests and disable after.
- Follow existing code style and conventions in each package.
- Include the Apache 2.0 license header in new files.
- Keep diffs minimal - avoid unrelated refactors or formatting changes.
- For new test files, follow existing test patterns in the same package.
- Commit your work after all.
