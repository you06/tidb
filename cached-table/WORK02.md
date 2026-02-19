# WORK02: Redesign cacheDB to global 1GB freecache with per-key min_cached_ts

## Description

Rewrite the cache database layer to use a single global 1GB `freecache.Cache` (LRU eviction, `github.com/coocood/freecache`) with per-key `min_cached_ts`-aware entries. This replaces the old per-table `kv.MemBuffer` approach with row-level caching.

Cache validity is governed by the lease-based invalidation protocol (WORK03). Invalidation is **per-key** — only the keys actually modified by the committing transaction are affected. The modified keys are recorded in `mysql.table_cache_invalidation` (WORK01) and forwarded to this layer by the background poller. Each invalidated key has a `min_cached_ts`; reads with `readTS <= min_cached_ts` must not serve or store cached data for that key. Non-invalidated keys in the same table remain cacheable.

## Files to modify

- `pkg/kv/cachedb.go`: Rewrite. Single global `freecache.Cache` (1GB). New methods: `UnionGet`/`BatchUnionGet` (with `readTS` parameter), `InvalidateKeys`, `RegisterCachedTable`, `UnregisterCachedTable`.
- `pkg/kv/cachedb_test.go`: Add comprehensive tests.

## Key design

```go
type cacheDB struct {
    cache          *freecache.Cache   // 1GB global, stores cached row data
    invalidatedKeys sync.Map          // map[string]uint64 - cacheKey -> min_cached_ts (per-key)
}

func (c *cacheDB) UnionGet(ctx context.Context, tid int64, readTS uint64, snapshot Snapshot, key Key) ([]byte, error)
func (c *cacheDB) BatchUnionGet(ctx context.Context, tid int64, readTS uint64, snapshot Snapshot, keys []Key) (map[string][]byte, error)
func (c *cacheDB) InvalidateKeys(keys []Key, minCachedTS uint64)
```

### Cache Key/Value Format in freecache

- Key: `bigEndian(tableID) + originalKVKey`
- Value: `bigEndian(cachedAtTS) + originalKVValue`

### On UnionGet

1. Build freecache key (`bigEndian(tid) + key`).
2. Check `invalidatedKeys` for this key. If present and `readTS <= min_cached_ts`, **bypass cache** — fetch from snapshot, do NOT cache the result, return.
3. If present and `readTS > min_cached_ts`, **compare-and-swap delete** the entry from `invalidatedKeys`: only delete if the stored value still equals the `min_cached_ts` we read in step 2. This prevents a race where a newer invalidation (with a higher `min_cached_ts`) arrives between our read and delete.
4. Check freecache. If hit and `cachedAtTS > min_cached_ts` (or key not in `invalidatedKeys`), return the cached value.
5. Otherwise (miss or stale entry), fetch from snapshot, cache with `cachedAtTS = readTS`, return.

CAS cleanup implementation using `sync.Map`:

```go
// Step 3: CAS delete — only remove if the value hasn't been updated by a concurrent invalidation.
if readTS > minCachedTS {
    c.invalidatedKeys.CompareAndDelete(cacheKeyStr, minCachedTS)
}
```

Note: `sync.Map.CompareAndDelete` is available since Go 1.20.

### On InvalidateKeys (called by the background invalidation poller, WORK03)

For each key in the invalidation:
1. Delete the key's entry from freecache (eager eviction of the known-stale entry).
2. **CAS-store** the key's `min_cached_ts` in `invalidatedKeys`: only store if the new value is greater than the existing value (or no entry exists). This ensures a newer invalidation is never overwritten by an older one arriving out of order.

```go
func (c *cacheDB) InvalidateKeys(keys []Key, minCachedTS uint64) {
    for _, key := range keys {
        cacheKeyStr := buildCacheKeyString(key)
        c.cache.Del([]byte(cacheKeyStr)) // eager eviction
        for {
            existing, loaded := c.invalidatedKeys.LoadOrStore(cacheKeyStr, minCachedTS)
            if !loaded {
                break // stored successfully (was empty)
            }
            if existing.(uint64) >= minCachedTS {
                break // existing entry is already newer
            }
            // existing is lower, try to upgrade
            if c.invalidatedKeys.CompareAndSwap(cacheKeyStr, existing, minCachedTS) {
                break
            }
            // CAS failed (concurrent update), retry
        }
    }
}
```

The `invalidatedKeys` map is **self-cleaning**: entries are removed via `CompareAndDelete` when a read with `readTS > min_cached_ts` comes in (step 3 in UnionGet). Since the invalidation window is short (one lease duration), entries are short-lived.

### Important

Keep the old `MemManager` interface temporarily (methods used by `EnablePointGetCache` for table-locked reads). Add new methods alongside. The old interface will be removed when `EnablePointGetCache` is updated.

## Testing

```bash
go test ./pkg/kv/... --tags=intest
```

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
