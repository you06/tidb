# WORK06: Add index lookup table lookup cache support

## Description

Add cache support to the index lookup table lookup path. Before sending cop requests for the table side, try the cache using row keys derived from handles. Cache results when they represent full rows.

## Files to modify

- `pkg/executor/distsql.go` (`tableWorker.executeTask`, line ~1841): This is the key interception point. The flow:
  1. Before `buildTableReader`, for cached tables, compute row keys from handles using `tablecodec.EncodeRowKeyWithHandle(physicalTableID, handle)`
  2. Try `cacheDB.BatchUnionGet(ctx, tableID, readTS, snapshot, rowKeys)` for all row keys
  3. For cache hits, decode row data from raw KV value using rowcodec (same decoder the table reader would use)
  4. For cache misses only, build the table reader with the remaining handles and send cop requests
  5. After the table reader returns, check cacheability: the result contains full rows (all columns, no projection/trim, no pushed-down conditions on the table side). If cacheable, encode each row as raw KV pair using `tablecodec.EncodeRow` and cache it with `cachedAtTS = readTS`.
  6. Merge cache-hit rows and table-reader rows, maintaining handle order if `keepOrder` is set
- `pkg/executor/distsql.go` (`IndexLookUpExecutor` struct): Add fields for cache support: `isCachedTable bool`, `readTS uint64`, `memCache kv.MemManager`
- `pkg/executor/builder.go` (`buildIndexLookUpReader`): Set cache fields on the executor when the table is cached.

## Cacheability check

```go
func isTableReaderResultCacheable(e *IndexLookUpExecutor) bool {
    // Cacheable if table side reads all columns (full row) and has no pushed-down conditions
    return len(e.columns) == len(e.table.Meta().Columns) && !hasTableSidePushDown(e.tableRequest)
}
```

### Important

Don't change the table reader's plan or pushdown. The cop request is only sent for cache-missed handles. The plan remains identical.

## Testing

Test index lookup on cached table. Verify table lookup side hits cache on subsequent identical lookups.

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
