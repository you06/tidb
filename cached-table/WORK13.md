# WORK13: Clean up CachedTableSupport and session context

## Description

Finalize the `CachedTableSupport` interface and clean up all session context references to the old cached table implementation.

## Files to modify

- `pkg/table/tblctx/table.go`: Finalize `CachedTableSupport` interface (if not already done in WORK03).
- `pkg/table/tblsession/table.go`: Finalize implementation.
- `pkg/ddl/reorg.go`: Update `CachedTableSupport` stub (return false).
- `pkg/lightning/backend/kv/context.go`: Update `CachedTableSupport` stub (return false).
- `pkg/infoschema/builder.go` (line 1079-1097): Remove the `if t, ok := ret.(table.CachedTable)` block and `Init()` call (no longer needed since cached tables are regular `TableCommon`).

## Testing

```bash
go test ./pkg/infoschema/... --tags=intest
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
