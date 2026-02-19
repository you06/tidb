# WORK16: Update existing tests and add comprehensive new tests

## Description

Update existing test expectations and add comprehensive new tests to verify the redesigned cached table implementation.

## Files to modify

- `tests/integrationtest/t/table/cache.test`: Update test expectations (no `UnionScan` in EXPLAIN, no size limit).
- `tests/integrationtest/t/ddl/db_cache.test`: Update DDL tests.
- `pkg/table/tables/cache_test.go` (if not deleted in WORK09, add new tests here or in a new test file).

## New tests to add

- Point get cache hit/miss cycle
- Batch point get cache behavior
- Index lookup table lookup cache behavior
- Full table scan does NOT use cache (goes to TiKV)
- Range scan does NOT use cache
- Per-key invalidation: write on one session, read on another sees new data
- Concurrent writes to cached table (invalidation entry ordering)
- ALTER TABLE CACHE / NOCACHE lifecycle
- EXPLAIN shows no UnionScan for cached tables
- Limit pushdown works on cached tables
- Partition table restriction still enforced
- Write latency: no lock waiting overhead
- 64MB table size limit is gone (large tables can be cached row-by-row)
- Cache eviction under memory pressure (1GB LRU)

## Testing

Skip.

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
