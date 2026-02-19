# WORK09: Remove CachedTable interface and cachedTable struct

## Description

Remove the old `CachedTable` interface and `cachedTable` struct. Cached tables now use regular `TableCommon` and caching is handled at the KV snapshot layer.

## Files to modify

- `pkg/table/table.go` (line 537-556): Delete the `CachedTable` interface entirely. Keep `ErrOptOnCacheTable` error.
- `pkg/table/tables/cache.go`: Delete the entire file (`cachedTable` struct, `tokenLimit`, `cacheData`, all methods).
- `pkg/table/tables/cache_test.go`: Delete or gut the file (old cache tests no longer apply).
- `pkg/table/tables/tables.go` (line 120-126, 144-145, 222-223): Remove `newCachedTable` function. In `TableFromMeta` and `MockTableFromMeta`, remove the cached table branch. Cached tables now return regular `TableCommon`.

## Testing

Verify `TableFromMeta` returns regular `TableCommon` for cached tables.

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
