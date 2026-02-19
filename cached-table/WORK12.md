# WORK12: Simplify DDL ALTER TABLE CACHE / NOCACHE

## Description

Simplify the DDL operations for `ALTER TABLE CACHE` and `ALTER TABLE NOCACHE` to work with the new lease-based per-key invalidation scheme instead of the old lock-based mechanism.

## Files to modify

- `pkg/ddl/executor.go` (line 6519-6574, `AlterTableCache`):
  - Remove `checkCacheTableSize()` call (no table size limit for row-level cache)
  - Remove `checkCacheTableSize` function (line 6576-6609)
  - Replace `REPLACE INTO mysql.table_cache_meta VALUES (%?, 'NONE', 0, 0)` with simplified insert (just `tid`)
  - After DDL succeeds, register table: `store.GetMemCache().RegisterCachedTable(tableID)`
- `pkg/ddl/executor.go` (line 6611-6633, `AlterTableNoCache`):
  - After DDL succeeds, unregister and clear cache: `store.GetMemCache().UnregisterCachedTable(tableID)` and `Delete(tableID)`
  - Clean up any remaining invalidation entries: `DELETE FROM mysql.table_cache_invalidation WHERE tid = tableID`
- `pkg/ddl/table.go` (line 1657-1733): Keep the 2-phase state machine (`Disable->Switching->Enable`) for safe schema propagation. Remove lock-related checks within each phase.

## Testing

```bash
go test ./pkg/ddl/... --tags=intest
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
