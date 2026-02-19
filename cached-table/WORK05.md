# WORK05: Integrate cachedSnapshot into PointGet and BatchPointGet executors

## Description

Replace the old `cacheTableSnapshot` wrapping in PointGet and BatchPointGet executors with the new `cachedSnapshot` wrapper from WORK04.

## Files to modify

- `pkg/executor/builder.go` (~line 80-108, `buildPointGet`): Replace old `cacheTableSnapshot` wrapping with new `cachedSnapshot`. Instead of calling `getCacheTable()`, check if table is cached and wrap the snapshot with `cachedSnapshot`.
- `pkg/executor/builder.go` (~line 5780-5808, `buildBatchPointGet`): Same replacement.
- `pkg/executor/builder.go`: Add helper `buildCachedSnapshot(snapshot kv.Snapshot, tables ...*model.TableInfo) kv.Snapshot` that creates a `cachedSnapshot` for all cached tables.

## Approach

```go
// In buildPointGet, replace lines 104-108:
if p.TblInfo.TableCacheStatusType == model.TableCacheStatusEnable {
    e.snapshot = b.wrapWithCachedSnapshot(e.snapshot, p.TblInfo)
}
```

The `wrapWithCachedSnapshot` method creates the `cachedSnapshot` wrapper for tables with `TableCacheStatusEnable`. No TiKV read is needed — cache validity is handled inside `cacheDB` via per-key `min_cached_ts` (WORK02).

## Testing

Run existing point get tests with cached tables. Verify cache hits on second read.

```bash
go test ./pkg/executor/... --tags=intest
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
