# WORK15: Clean up variables, metrics, configuration

## Description

Clean up session variables, metrics, and configuration related to the old cached table implementation.

## Files to modify

- `pkg/sessionctx/vardef/tidb_vars.go`: Deprecate `TableCacheLease` variable (keep for compatibility but make it a no-op).
- `pkg/sessionctx/stmtctx/stmtctx.go`: Remove `WaitLockLeaseTime` field.
- `pkg/executor/adapter.go`: Remove `WaitLockLeaseTime` from slow log / execution details.
- `pkg/metrics/`: Remove or repurpose `LoadTableCacheDurationHistogram`.
- `pkg/sessionctx/stmtctx/stmtctx.go`: Keep `ReadFromTableCache` field (repurpose to mean "had cache hits").

## Testing

```bash
go test ./pkg/executor/... --tags=intest
go test ./pkg/sessionctx/... --tags=intest
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
