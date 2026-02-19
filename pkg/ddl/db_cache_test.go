// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
	"github.com/stretchr/testify/require"
)

func checkTableCacheStatus(t *testing.T, tk *testkit.TestKit, dbName, tableName string, status model.TableCacheStatusType) {
	tb := external.GetTableByName(t, tk, dbName, tableName)
	dom := domain.GetDomain(tk.Session())
	err := dom.Reload()
	require.NoError(t, err)
	require.Equal(t, status, tb.Meta().TableCacheStatusType)
}

func TestAlterTableCache(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	dom.SetStatsUpdating(true)

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk2.MustExec("use test")
	/* Test of cache table */
	tk.MustExec("create table t1 ( n int auto_increment primary key)")
	tk.MustGetErrCode("alter table t1 ca", errno.ErrParse)
	tk.MustGetErrCode("alter table t2 cache", errno.ErrNoSuchTable)
	tk.MustExec("alter table t1 cache")
	checkTableCacheStatus(t, tk, "test", "t1", model.TableCacheStatusEnable)
	tk.MustExec("alter table t1 nocache")
	tk.MustExec("drop table if exists t1")
	if kerneltype.IsClassic() {
		tk.MustExec("set global tidb_enable_metadata_lock=0")
		/*Test can't skip schema checker*/
		tk.MustExec("drop table if exists t1,t2")
		tk.MustExec("CREATE TABLE t1 (a int)")
		tk.MustExec("CREATE TABLE t2 (a int)")
		tk.MustExec("begin")
		tk.MustExec("insert into t1 set a=1;")
		tk2.MustExec("alter table t1 cache;")
		tk.MustGetDBError("commit", domain.ErrInfoSchemaChanged)
	} else {
		tk.MustExec("drop table if exists t1,t2")
		tk.MustExec("CREATE TABLE t1 (a int)")
		tk.MustExec("CREATE TABLE t2 (a int)")
		tk.MustExec("insert into t1 set a=1;")
		tk2.MustExec("alter table t1 cache;")
	}
	/* Test can skip schema checker */
	tk.MustExec("begin")
	tk.MustExec("alter table t1 nocache")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("CREATE TABLE t1 (a int)")
	tk.MustExec("insert into t1 set a=2;")
	tk2.MustExec("alter table t2 cache")
	tk.MustExec("commit")
	// Test if a table is not exists
	tk.MustExec("drop table if exists t")
	tk.MustGetErrCode("alter table t cache", errno.ErrNoSuchTable)
	tk.MustExec("create table t (a int)")
	tk.MustExec("alter table t cache")
	// Multiple alter cache is okay
	tk.MustExec("alter table t cache")
	tk.MustExec("alter table t cache")
	// Test a temporary table
	tk.MustExec("alter table t nocache")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create temporary table t (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("drop table if exists tmp1")
	// local temporary table alter is not supported
	tk.MustGetErrCode("alter table t cache", errno.ErrUnsupportedDDLOperation)
	// test global temporary table
	tk.MustExec("create global temporary table tmp1 " +
		"(id int not null primary key, code int not null, value int default null, unique key code(code))" +
		"on commit delete rows")
	tk.MustGetErrMsg("alter table tmp1 cache", dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("alter temporary table cache").Error())
	// create table like
	tk.MustExec("drop table t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("alter table t cache")
	tk.MustExec("create table t3 like t")
	checkTableCacheStatus(t, tk, "test", "t", model.TableCacheStatusEnable)
	checkTableCacheStatus(t, tk, "test", "t3", model.TableCacheStatusDisable)
}

func TestIssue34069(t *testing.T) {
	testIssue34069(t, sem.V1)
	testIssue34069(t, sem.V2)
}

func testIssue34069(t *testing.T, semVer string) {
	store := testkit.CreateMockStore(t)
	defer sem.SwitchToSEMForTest(t, semVer)()

	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test;")
	tk.MustExec("create table t_34069 (t int);")
	// No error when SEM is enabled.
	tk.MustExec("alter table t_34069 cache")
}

func TestCacheTablePointGetCacheCycle(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_point_cache")
	tk.MustExec("create table t_point_cache (id int primary key, v int)")
	tk.MustExec("insert into t_point_cache values(1, 100), (2, 200)")
	tk.MustExec("alter table t_point_cache cache")

	// Point get by PK.
	tk.MustQuery("select * from t_point_cache where id = 1").Check(testkit.Rows("1 100"))
	tk.MustQuery("select * from t_point_cache where id = 2").Check(testkit.Rows("2 200"))
	// Repeated reads should return same results (cache hit).
	tk.MustQuery("select * from t_point_cache where id = 1").Check(testkit.Rows("1 100"))
	tk.MustQuery("select * from t_point_cache where id = 2").Check(testkit.Rows("2 200"))

	tk.MustExec("alter table t_point_cache nocache")
}

func TestCacheTableBatchPointGetCacheBehavior(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_batch_cache")
	tk.MustExec("create table t_batch_cache (id int primary key, u int unique, v int)")
	tk.MustExec("insert into t_batch_cache values(1, 11, 101), (2, 12, 102), (3, 13, 103)")
	tk.MustExec("alter table t_batch_cache cache")

	// Batch point get by PK.
	tk.MustQuery("select * from t_batch_cache where id in (1, 3)").Sort().Check(testkit.Rows("1 11 101", "3 13 103"))
	// Batch point get by unique key.
	tk.MustQuery("select * from t_batch_cache where u in (11, 13)").Sort().Check(testkit.Rows("1 11 101", "3 13 103"))
	// Non-existent keys in batch.
	tk.MustQuery("select * from t_batch_cache where id in (1, 999)").Check(testkit.Rows("1 11 101"))

	tk.MustExec("alter table t_batch_cache nocache")
}

func TestCacheTableExplainNoUnionScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_explain_cache")
	tk.MustExec("create table t_explain_cache (id int primary key, u int unique, v int, key idx_v(v))")
	tk.MustExec("insert into t_explain_cache values(1, 11, 101), (2, 12, 102)")
	tk.MustExec("alter table t_explain_cache cache")

	// Point get by PK: should NOT contain UnionScan.
	rows := tk.MustQuery("explain select * from t_explain_cache where id = 1").Rows()
	for _, row := range rows {
		require.NotContains(t, row[0].(string), "UnionScan", "EXPLAIN point get should not contain UnionScan")
	}

	// Batch point get: should NOT contain UnionScan.
	rows = tk.MustQuery("explain select * from t_explain_cache where id in (1, 2)").Rows()
	for _, row := range rows {
		require.NotContains(t, row[0].(string), "UnionScan", "EXPLAIN batch point get should not contain UnionScan")
	}

	// Unique key point get: should NOT contain UnionScan.
	rows = tk.MustQuery("explain select * from t_explain_cache where u = 11").Rows()
	for _, row := range rows {
		require.NotContains(t, row[0].(string), "UnionScan", "EXPLAIN unique key point get should not contain UnionScan")
	}

	// Full table scan: should NOT contain UnionScan.
	rows = tk.MustQuery("explain select * from t_explain_cache").Rows()
	for _, row := range rows {
		require.NotContains(t, row[0].(string), "UnionScan", "EXPLAIN full scan should not contain UnionScan")
	}

	// Range scan: should NOT contain UnionScan.
	rows = tk.MustQuery("explain select * from t_explain_cache where id > 1").Rows()
	for _, row := range rows {
		require.NotContains(t, row[0].(string), "UnionScan", "EXPLAIN range scan should not contain UnionScan")
	}

	// Index lookup: should NOT contain UnionScan.
	rows = tk.MustQuery("explain select * from t_explain_cache where v = 101").Rows()
	for _, row := range rows {
		require.NotContains(t, row[0].(string), "UnionScan", "EXPLAIN index lookup should not contain UnionScan")
	}

	tk.MustExec("alter table t_explain_cache nocache")
}

func TestCacheTableFullAndRangeScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_scan_cache")
	tk.MustExec("create table t_scan_cache (id int primary key, v int)")
	tk.MustExec("insert into t_scan_cache values(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
	tk.MustExec("alter table t_scan_cache cache")

	// Full table scan works correctly.
	tk.MustQuery("select * from t_scan_cache order by id").Check(testkit.Rows(
		"1 10", "2 20", "3 30", "4 40", "5 50",
	))

	// Range scan works correctly.
	tk.MustQuery("select * from t_scan_cache where id > 2 order by id").Check(testkit.Rows(
		"3 30", "4 40", "5 50",
	))
	tk.MustQuery("select * from t_scan_cache where id between 2 and 4 order by id").Check(testkit.Rows(
		"2 20", "3 30", "4 40",
	))

	tk.MustExec("alter table t_scan_cache nocache")
}

func TestCacheTableLimitPushdown(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_limit_cache")
	tk.MustExec("create table t_limit_cache (id int primary key, v int)")
	tk.MustExec("insert into t_limit_cache values(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
	tk.MustExec("alter table t_limit_cache cache")

	// Limit pushdown works on cached tables.
	tk.MustQuery("select * from t_limit_cache order by id limit 3").Check(testkit.Rows(
		"1 10", "2 20", "3 30",
	))
	tk.MustQuery("select * from t_limit_cache where id > 1 order by id limit 2").Check(testkit.Rows(
		"2 20", "3 30",
	))

	// Verify EXPLAIN shows Limit operator (not blocked by cache).
	rows := tk.MustQuery("explain select * from t_limit_cache order by id limit 3").Rows()
	hasLimit := false
	for _, row := range rows {
		if id, ok := row[0].(string); ok {
			if len(id) >= 5 && id[:5] == "Limit" {
				hasLimit = true
			}
		}
	}
	require.True(t, hasLimit, "EXPLAIN should show Limit operator for cached table query")

	tk.MustExec("alter table t_limit_cache nocache")
}

func TestCacheTableIndexLookupBehavior(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_idx_cache")
	tk.MustExec("create table t_idx_cache (id int primary key, a int, b int, key idx_a(a))")
	tk.MustExec("insert into t_idx_cache values(1, 10, 100), (2, 10, 200), (3, 20, 300), (4, 20, 400)")
	tk.MustExec("alter table t_idx_cache cache")

	// Index lookup returns correct results.
	tk.MustQuery("select * from t_idx_cache where a = 10 order by id").Check(testkit.Rows(
		"1 10 100", "2 10 200",
	))
	tk.MustQuery("select * from t_idx_cache where a = 20 order by id").Check(testkit.Rows(
		"3 20 300", "4 20 400",
	))

	// Index range scan.
	tk.MustQuery("select * from t_idx_cache where a >= 10 and a <= 20 order by id").Check(testkit.Rows(
		"1 10 100", "2 10 200", "3 20 300", "4 20 400",
	))

	tk.MustExec("alter table t_idx_cache nocache")
}

func TestCacheTableLifecycle(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_life_cache")
	tk.MustExec("create table t_life_cache (id int primary key, v int)")
	tk.MustExec("insert into t_life_cache values(1, 100), (2, 200)")

	// Initially disabled.
	checkTableCacheStatus(t, tk, "test", "t_life_cache", model.TableCacheStatusDisable)
	tk.MustQuery("select * from t_life_cache where id = 1").Check(testkit.Rows("1 100"))

	// Enable cache.
	tk.MustExec("alter table t_life_cache cache")
	checkTableCacheStatus(t, tk, "test", "t_life_cache", model.TableCacheStatusEnable)
	tk.MustQuery("select * from t_life_cache where id = 1").Check(testkit.Rows("1 100"))
	tk.MustQuery("select * from t_life_cache where id = 2").Check(testkit.Rows("2 200"))

	// Disable cache.
	tk.MustExec("alter table t_life_cache nocache")
	checkTableCacheStatus(t, tk, "test", "t_life_cache", model.TableCacheStatusDisable)
	tk.MustQuery("select * from t_life_cache where id = 1").Check(testkit.Rows("1 100"))
	tk.MustQuery("select * from t_life_cache where id = 2").Check(testkit.Rows("2 200"))

	// Re-enable cache.
	tk.MustExec("alter table t_life_cache cache")
	checkTableCacheStatus(t, tk, "test", "t_life_cache", model.TableCacheStatusEnable)
	tk.MustQuery("select * from t_life_cache where id = 1").Check(testkit.Rows("1 100"))
	tk.MustQuery("select * from t_life_cache where id = 2").Check(testkit.Rows("2 200"))

	tk.MustExec("alter table t_life_cache nocache")
}

func TestCacheTablePartitionRestriction(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Hash partition cannot be cached.
	tk.MustExec("drop table if exists t_hash_part")
	tk.MustExec("create table t_hash_part (a int, b int) partition by hash(a) partitions 3")
	tk.MustGetErrCode("alter table t_hash_part cache", errno.ErrOptOnCacheTable)

	// Range partition cannot be cached.
	tk.MustExec("drop table if exists t_range_part")
	tk.MustExec("create table t_range_part (c1 int) partition by range(c1) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustGetErrCode("alter table t_range_part cache", errno.ErrOptOnCacheTable)

	// List partition cannot be cached.
	tk.MustExec("drop table if exists t_list_part")
	tk.MustExec("create table t_list_part (id int) partition by list(id) (partition p0 values in (1,2), partition p1 values in (3,4))")
	tk.MustGetErrCode("alter table t_list_part cache", errno.ErrOptOnCacheTable)
}

func TestCacheTableNoSizeLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_large_cache")
	tk.MustExec("create table t_large_cache (id int primary key, data varchar(1000))")

	// Insert rows with large data values.
	for i := 1; i <= 50; i++ {
		tk.MustExec("insert into t_large_cache values(?, repeat('x', 1000))", i)
	}

	// ALTER TABLE CACHE should succeed with no size limit.
	tk.MustExec("alter table t_large_cache cache")
	checkTableCacheStatus(t, tk, "test", "t_large_cache", model.TableCacheStatusEnable)

	// Point get works on large rows.
	tk.MustQuery("select id, length(data) from t_large_cache where id = 1").Check(testkit.Rows("1 1000"))
	tk.MustQuery("select id, length(data) from t_large_cache where id = 50").Check(testkit.Rows("50 1000"))

	// Batch point get works.
	tk.MustQuery("select count(*) from t_large_cache where id in (1, 10, 20, 30, 40, 50)").Check(testkit.Rows("6"))

	tk.MustExec("alter table t_large_cache nocache")
}

func TestCacheTableDMLWhileCached(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_dml_cache")
	tk.MustExec("create table t_dml_cache (id int primary key, v int)")
	tk.MustExec("alter table t_dml_cache cache")

	// DML operations should succeed on cached tables without error.
	tk.MustExec("insert into t_dml_cache values(1, 100)")
	tk.MustExec("insert into t_dml_cache values(2, 200)")
	tk.MustExec("update t_dml_cache set v = 300 where id = 1")
	tk.MustExec("delete from t_dml_cache where id = 2")
	tk.MustExec("insert into t_dml_cache values(3, 400)")

	// Disable cache and verify all DML changes are durable.
	tk.MustExec("alter table t_dml_cache nocache")
	tk.MustQuery("select * from t_dml_cache order by id").Check(testkit.Rows("1 300", "3 400"))
}
