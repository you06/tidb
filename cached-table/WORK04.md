# WORK04: Create cachedSnapshot wrapper

## Description

Create a `cachedSnapshot` wrapper that implements `kv.Snapshot` and intercepts `Get`/`BatchGet` for cached tables, routing through `cacheDB.UnionGet`/`BatchUnionGet`.

The snapshot's own `readTS` is passed to `cacheDB`, which internally checks it against per-key `min_cached_ts` (managed by the background invalidation poller from WORK03 and stored via WORK01) to decide whether to serve from cache or bypass.

### Scans bypass cache (intentional)

`Iter`/`IterReverse` always pass through to the underlying TiKV snapshot — scans are **not** cached. This is an intentional design tradeoff:

- **Row-level caching is point-access-oriented.** The freecache stores individual row KV entries keyed by `tableID + rowKey`. Scan results would need to be decomposed into individual rows for caching and reassembled on read, adding complexity with little benefit since scan access patterns are typically less repetitive than point lookups.
- **No stale range guarantees.** Caching scan results requires tracking which key ranges are cached and invalidating ranges on writes. Per-key invalidation (WORK01/WORK02) does not cover range completeness — a scan could return a mix of cached and uncached rows with no way to know if a newly inserted row falls within a "cached" range.
- **Optimizer pushdown is restored.** With `UnionScan` removed (WORK07), scans on cached tables use the same plan as regular tables — TiKV coprocessor pushdown (limit, aggregation, TopN, etc.) works normally. Caching scan results would lose these pushdown benefits.
- **Point gets dominate the cached table workload.** Cached tables are designed for small, frequently-read reference data. The common access pattern is point get by primary key or unique index, which is fully cached.

## Files to create

- `pkg/executor/cached_snapshot.go`: Implements `kv.Snapshot` wrapper.

## Key design

```go
type cachedSnapshot struct {
    kv.Snapshot
    memCache     kv.MemManager
    cachedTables map[int64]struct{} // set of cached table IDs
    readTS       uint64             // this snapshot's timestamp
}

func (s *cachedSnapshot) Get(ctx context.Context, key kv.Key) ([]byte, error) {
    tableID := tablecodec.DecodeTableID(key)
    if _, ok := s.cachedTables[tableID]; ok {
        return s.memCache.UnionGet(ctx, tableID, s.readTS, s.Snapshot, key)
    }
    return s.Snapshot.Get(ctx, key)
}

func (s *cachedSnapshot) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
    // Group keys by cached/uncached. Cached keys go through BatchUnionGet with s.readTS.
    // Uncached keys go through s.Snapshot.BatchGet. Merge results.
}
```

### Creating the cachedSnapshot

When creating the `cachedSnapshot`, simply collect the set of cached table IDs and the snapshot's `readTS`:

```go
func newCachedSnapshot(snapshot kv.Snapshot, memCache kv.MemManager, cachedTableIDs []int64) *cachedSnapshot {
    tables := make(map[int64]struct{}, len(cachedTableIDs))
    for _, id := range cachedTableIDs {
        tables[id] = struct{}{}
    }
    return &cachedSnapshot{
        Snapshot:     snapshot,
        memCache:     memCache,
        cachedTables: tables,
        readTS:       snapshot.StartTS(),
    }
}
```

The `cacheDB.UnionGet` (WORK02) handles the `min_cached_ts` check internally — if `readTS <= min_cached_ts`, it bypasses the cache and does not store the result.

## Testing

```bash
go test ./pkg/executor/... --tags=intest
```

Unit tests for `cachedSnapshot`.

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
