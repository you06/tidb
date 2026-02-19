# WORK14: Update mysql.table_cache_meta system table schema

## Description

Simplify the `mysql.table_cache_meta` system table schema by removing the lock-related columns. Invalidation is now handled by `mysql.table_cache_invalidation` (WORK01); this table only tracks which tables are cached.

## Files to modify

- `pkg/meta/metadef/system_tables_def.go` (line 360-366): Change `CreateTableCacheMetaTable` to new schema:

```sql
CREATE TABLE IF NOT EXISTS mysql.table_cache_meta (
    tid bigint(11) NOT NULL DEFAULT 0,
    PRIMARY KEY (tid)
);
```

Remove `lock_type`, `lease`, `oldReadLease` columns. Invalidation is handled by `mysql.table_cache_invalidation` (WORK01).

- `pkg/session/bootstrap.go`: Add upgrade function that drops old columns for existing installations.
- `pkg/session/upgrade_def.go`: Add new version constant.

## Testing

Test bootstrap upgrade path.

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
