# WORK11: Remove cachedTableRenewLease from session commit path

## Description

Remove the old `cachedTableRenewLease` struct and all its methods from the session commit path. The precommit hook from WORK03 is the only cached table logic needed in `doCommit`.

## Files to modify

- `pkg/session/session.go` (line 591-632): Delete the `cachedTableRenewLease` struct and all its methods (`start`, `stop`, `commitTSCheck`).
- `pkg/session/session.go` (line 568-579): Remove the old cached table write lock acquisition. The precommit hook from WORK03 is the only cached table logic in `doCommit`.

## Testing

Verify writes to cached tables work without write lock acquisition. Verify commit completes quickly (no `WaitLockLeaseTime`).

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
