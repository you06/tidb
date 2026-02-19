# WORK01: Define invalidation table and per-key invalidation storage

## Description

Define the system table that stores per-key cache invalidation entries. When a transaction modifies rows in a cached table, the modified KV keys and their `min_cached_ts` are written to this table in a separate internal transaction. A background poller on each TiDB node reads these entries and forwards them to `cacheDB.InvalidateKeys` (WORK02).

This table is the bridge between the write path (WORK03) and the read-side cache invalidation (WORK02).

## Files to modify

- `pkg/meta/metadef/system_tables_def.go`: Add `CreateTableCacheInvalidationTable` definition.
- `pkg/session/bootstrap.go`: Add bootstrap logic to create the table.
- `pkg/session/upgrade_def.go`: Add version constant for the new table.

## Table schema

```sql
CREATE TABLE IF NOT EXISTS mysql.table_cache_invalidation (
    tid BIGINT NOT NULL,
    cache_key VARBINARY(4096) NOT NULL,
    min_cached_ts BIGINT UNSIGNED NOT NULL,
    INDEX idx_tid_ts (tid, min_cached_ts)
);
```

Column semantics:
- `tid`: The table ID of the cached table whose row was modified.
- `cache_key`: The raw KV key (`tablecodec.EncodeRowKeyWithHandle(tableID, handle)`) of the modified row, stored as binary.
- `min_cached_ts`: Reads with `readTS <= min_cached_ts` must not serve cached data for this key. Equals the `commitTS` of the data transaction (set by the precommit hook, see WORK03).

No auto-increment primary key — rows are inserted in bulk and cleaned up by timestamp range. The composite index `(tid, min_cached_ts)` supports both per-table polling and timestamp-based cleanup.

## Writer flow (called from WORK03 precommit hook)

When the precommit hook fires with a known `commitTS`:

```go
func writeInvalidationEntries(ctx context.Context, sctx sessionctx.Context, store kv.Storage,
    modifiedKeys map[int64][]kv.Key, commitTS uint64) error {
    // Open a separate internal SQL session (not the committing transaction).
    // For each cached table's modified keys, batch-insert into mysql.table_cache_invalidation.
    //
    // INSERT INTO mysql.table_cache_invalidation (tid, cache_key, min_cached_ts) VALUES
    //   (tableID1, key1, commitTS),
    //   (tableID1, key2, commitTS),
    //   (tableID2, key3, commitTS), ...
    //
    // This transaction commits independently and becomes visible to all nodes immediately.
}
```

The `modifiedKeys` map is built from the transaction's mutation set: iterate `txn.GetMemBuffer()`, decode each key's table ID, and collect keys belonging to cached tables (tracked via `sessVars.TxnCtx.CachedTables`).

## Background poller flow (reads by WORK03 poller)

Each TiDB node's background goroutine polls every half-lease:

```go
func (p *invalidationPoller) poll(ctx context.Context) error {
    // SELECT tid, cache_key, min_cached_ts
    // FROM mysql.table_cache_invalidation
    // WHERE min_cached_ts > lastPolledTS
    // ORDER BY min_cached_ts ASC
    //
    // For each row, call:
    //   cacheDB.InvalidateKeys([]kv.Key{cacheKey}, minCachedTS)
    //
    // Update lastPolledTS to the max min_cached_ts seen.
}
```

## Cleanup strategy

A background goroutine (can share the poller goroutine) periodically deletes expired entries:

```go
// Entries are expired when min_cached_ts is older than (now - 2*lease).
// At that point, all nodes have already processed the invalidation and
// no read with readTS <= min_cached_ts can arrive.
DELETE FROM mysql.table_cache_invalidation
WHERE min_cached_ts < currentTSO - 2 * leaseDurationInTSUnits
```

The cleanup interval can be longer (e.g. every 10 seconds) since expired rows are harmless — they just waste storage.

## Testing

```bash
go test ./pkg/session/... --tags=intest
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
