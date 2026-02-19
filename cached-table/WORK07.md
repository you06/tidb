# WORK07: Remove LogicalUnionScan injection for cached tables

## Description

This is the key switch: cached tables no longer get `LogicalUnionScan`. Plans are identical to non-cached tables. Limit pushdown, aggregation pushdown, and all other optimizations work normally.

## Files to modify

- `pkg/planner/core/logical_plan_builder.go` (line 4731): Change:

```go
// Before:
if dirty || tableInfo.TempTableType == model.TempTableLocal || tableInfo.TableCacheStatusType == model.TableCacheStatusEnable {
// After:
if dirty || tableInfo.TempTableType == model.TempTableLocal {
```

## Testing

Verify EXPLAIN output for cached table queries no longer shows `UnionScan`. Verify limit pushdown works. Run all planner tests.

```bash
go test ./pkg/planner/... --tags=intest
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
