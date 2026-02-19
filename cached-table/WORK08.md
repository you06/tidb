# WORK08: Remove cached table handling from UnionScanExec and executor builder

## Description

Remove all cached table-specific handling from `UnionScanExec` and the executor builder. `UnionScan` should only handle dirty transactions and temp tables after this change.

## Files to modify

- `pkg/executor/union_scan.go` (line 59-61): Remove `cacheTable kv.MemBuffer` field.
- `pkg/executor/union_scan.go` (line 243-246): Remove `if us.cacheTable != nil` check in `getSnapshotRow`.
- `pkg/executor/builder.go` (line 1630-1648): Remove `handleCachedTable` method.
- `pkg/executor/builder.go`: Remove all `us.handleCachedTable(...)` calls in `buildUnionScanExec`.
- `pkg/executor/builder.go` (line 6137-6156): Remove `getCacheTable` method.
- `pkg/executor/batch_point_get.go` (line 126-179): Remove `cacheTableSnapshot` struct and its methods.
- `pkg/executor/batch_point_get.go` (line 177): Remove `MockNewCacheTableSnapShot`.

## Testing

Verify `UnionScan` still works for dirty transactions and temp tables.

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
