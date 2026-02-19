# WORK10: Remove StateRemote lock mechanism

## Description

Remove the `StateRemote` interface and `stateRemoteHandle` that implement the lock-based invalidation mechanism (`READ`/`WRITE`/`INTEND` locks on `mysql.table_cache_meta`). This is no longer needed with lease-based per-key invalidation.

## Files to modify

- `pkg/table/tables/state_remote.go`: Delete the entire file (`StateRemote` interface, `stateRemoteHandle`, `CachedTableLockType`, all lock operations).
- `pkg/table/tables/state_remote_test.go`: Delete the entire file.

## Testing

```bash
go test ./pkg/table/... --tags=intest
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
