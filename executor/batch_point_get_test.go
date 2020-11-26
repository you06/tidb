// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"fmt"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
)

type testBatchPointGetSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func newStoreWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := mockstore.NewMockStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	session.SetSchemaLease(0)
	session.DisableStats4Test()

	dom, err := session.BootstrapSession(store)
	if err != nil {
		return nil, nil, err
	}
	return store, dom, errors.Trace(err)
}

func (s *testBatchPointGetSuite) SetUpSuite(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.store = store
	s.dom = dom
}

func (s *testBatchPointGetSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *testBatchPointGetSuite) TestBatchPointGetExec(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key auto_increment not null, b int, c int, unique key idx_abc(a, b, c))")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 5)")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
		"3 3 3",
		"4 4 5",
	))
	tk.MustQuery("select a, b, c from t where (a, b, c) in ((1, 1, 1), (1, 1, 1), (1, 1, 1))").Check(testkit.Rows(
		"1 1 1",
	))
	tk.MustQuery("select a, b, c from t where (a, b, c) in ((1, 1, 1), (2, 2, 2), (1, 1, 1))").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
	))
	tk.MustQuery("select a, b, c from t where (a, b, c) in ((1, 1, 1), (2, 2, 2), (100, 1, 1))").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
	))
	tk.MustQuery("select a, b, c from t where (a, b, c) in ((1, 1, 1), (2, 2, 2), (100, 1, 1), (4, 4, 5))").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
		"4 4 5",
	))
	tk.MustQuery("select * from t where a in (1, 2, 4, 1, 2)").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
		"4 4 5",
	))
	tk.MustQuery("select * from t where a in (1, 2, 4, 1, 2, 100)").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
		"4 4 5",
	))
	tk.MustQuery("select a from t where a in (1, 2, 4, 1, 2, 100)").Check(testkit.Rows(
		"1",
		"2",
		"4",
	))
}

func (s *testBatchPointGetSuite) TestBatchPointGetInTxn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key auto_increment, name varchar(30))")

	// Fix a bug that BatchPointGetExec doesn't consider membuffer data in a transaction.
	tk.MustExec("begin")
	tk.MustExec("insert into t values (4, 'name')")
	tk.MustQuery("select * from t where id in (4)").Check(testkit.Rows("4 name"))
	tk.MustQuery("select * from t where id in (4) for update").Check(testkit.Rows("4 name"))
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (4, 'name')")
	tk.MustQuery("select * from t where id in (4)").Check(testkit.Rows("4 name"))
	tk.MustQuery("select * from t where id in (4) for update").Check(testkit.Rows("4 name"))
	tk.MustExec("rollback")

	tk.MustExec("create table s (a int, b int, c int, primary key (a, b))")
	tk.MustExec("insert s values (1, 1, 1), (3, 3, 3), (5, 5, 5)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("update s set c = 10 where a = 3")
	tk.MustQuery("select * from s where (a, b) in ((1, 1), (2, 2), (3, 3)) for update").Check(testkit.Rows("1 1 1", "3 3 10"))
	tk.MustExec("rollback")
}

func (s *testBatchPointGetSuite) TestBatchPointGetCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table customers (id int primary key, token varchar(255) unique)")
	tk.MustExec("INSERT INTO test.customers (id, token) VALUES (28, '07j')")
	tk.MustExec("INSERT INTO test.customers (id, token) VALUES (29, '03j')")
	tk.MustExec("BEGIN")
	tk.MustQuery("SELECT id, token FROM test.customers WHERE id IN (28)")
	tk.MustQuery("SELECT id, token FROM test.customers WHERE id IN (28, 29);").Check(testkit.Rows("28 07j", "29 03j"))
}

func (s *testBatchPointGetSuite) TestIssue18843(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t18843 ( id bigint(10) primary key, f varchar(191) default null, unique key `idx_f` (`f`))")
	tk.MustExec("insert into t18843 values (1, '')")
	tk.MustQuery("select * from t18843 where f in (null)").Check(testkit.Rows())

	tk.MustExec("insert into t18843 values (2, null)")
	tk.MustQuery("select * from t18843 where f in (null)").Check(testkit.Rows())
	tk.MustQuery("select * from t18843 where f is null").Check(testkit.Rows("2 <nil>"))
}

func (s *testBatchPointGetSuite) TestBatchPointGetUnsignedHandleWithSort(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (id bigint(20) unsigned, primary key(id))")
	tk.MustExec("insert into t2 values (8738875760185212610)")
	tk.MustExec("insert into t2 values (9814441339970117597)")
	tk.MustExec("insert into t2 values (1)")
	tk.MustQuery("select id from t2 where id in (8738875760185212610, 1, 9814441339970117597) order by id").Check(testkit.Rows("1", "8738875760185212610", "9814441339970117597"))
	tk.MustQuery("select id from t2 where id in (8738875760185212610, 1, 9814441339970117597) order by id desc").Check(testkit.Rows("9814441339970117597", "8738875760185212610", "1"))
}

func (s *testBatchPointGetSuite) TestBatchPointGetLockExistKey(c *C) {
	var wg sync.WaitGroup
	testLock := func(tk1, tk2 *testkit.TestKit, rc bool, key string, tableName string) {
		doneCh := make(chan struct{}, 1)

		tk1.MustExec("use test")
		tk2.MustExec("use test")
		tk1.MustExec("set session tidb_enable_clustered_index = 0")

		tk1.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
		tk1.MustExec(fmt.Sprintf("create table %s(id int, v int, k int, %s key0(id, v))", tableName, key))
		tk1.MustExec(fmt.Sprintf("insert into %s values(1, 1, 1), (2, 2, 2)", tableName))

		if rc {
			tk1.MustExec("set tx_isolation = 'READ-COMMITTED'")
			tk2.MustExec("set tx_isolation = 'READ-COMMITTED'")
		}

		tk1.MustExec("begin pessimistic")
		tk2.MustExec("begin pessimistic")

		if !rc {
			// lock exist key only for repeatable read
			tk1.MustExec(fmt.Sprintf("select * from %s where (id, v) in ((1, 1), (2, 2)) for update", tableName))
		} else {
			// read committed will not lock non-exist key
			tk1.MustExec(fmt.Sprintf("select * from %s where (id, v) in ((1, 1), (2, 2), (3, 3)) for update", tableName))
		}
		tk2.MustExec(fmt.Sprintf("insert into %s values(3, 3, 3)", tableName))
		go func() {
			tk2.MustExec(fmt.Sprintf("insert into %s values(1, 1, 3)", tableName))
			doneCh <- struct{}{}
		}()
		time.Sleep(50 * time.Millisecond)
		tk1.MustExec(fmt.Sprintf("update %s set v = 2 where id = 1 and v = 1", tableName))

		tk1.MustExec("commit")
		<-doneCh
		tk2.MustExec("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 2 2",
			"3 3 3",
			"1 1 3",
		))

		wg.Done()
	}

	for i, one := range []struct {
		rc  bool
		key string
	}{
		{rc: false, key: "primary key"},
		{rc: false, key: "unique key"},
		{rc: true, key: "primary key"},
		{rc: true, key: "unique key"},
	} {
		wg.Add(1)
		tableName := fmt.Sprintf("t_%d", i)
		go testLock(testkit.NewTestKit(c, s.store), testkit.NewTestKit(c, s.store), one.rc, one.key, tableName)
	}
	wg.Wait()
}
