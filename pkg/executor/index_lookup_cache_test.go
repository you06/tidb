// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockCacheStorage is a minimal kv.Storage that only implements GetSnapshot.
type mockCacheStorage struct {
	kv.Storage
	snapshot kv.Snapshot
}

func (s *mockCacheStorage) GetSnapshot(_ kv.Version) kv.Snapshot {
	return s.snapshot
}

// buildTestTableInfo creates a simple TableInfo with two columns: id (PK) and val.
func buildTestTableInfo() *model.TableInfo {
	colID := &model.ColumnInfo{
		ID:        1,
		Name:      ast.NewCIStr("id"),
		Offset:    0,
		State:     model.StatePublic,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
	}
	colID.AddFlag(mysql.PriKeyFlag)
	colID.AddFlag(mysql.NotNullFlag)

	colVal := &model.ColumnInfo{
		ID:        2,
		Name:      ast.NewCIStr("val"),
		Offset:    1,
		State:     model.StatePublic,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
	}

	return &model.TableInfo{
		ID:          100,
		Name:        ast.NewCIStr("t"),
		PKIsHandle:  true,
		Columns:     []*model.ColumnInfo{colID, colVal},
		State:       model.StatePublic,
		IsCommonHandle: false,
	}
}

// buildTestSchema creates an expression.Schema matching the test table.
func buildTestSchema(tblInfo *model.TableInfo) *expression.Schema {
	schema := expression.NewSchema()
	for i, col := range tblInfo.Columns {
		schema.Append(&expression.Column{
			UniqueID: int64(i + 1),
			ID:       col.ID,
			RetType:  col.FieldType.Clone(),
		})
	}
	return schema
}

// encodeTestRow encodes a row with the given val column value using new row format.
func encodeTestRow(t *testing.T, val int64) []byte {
	var encoder rowcodec.Encoder
	// Only encode non-PK columns (PK value comes from the handle).
	colIDs := []int64{2} // column "val"
	datums := []types.Datum{types.NewIntDatum(val)}
	encoded, err := encoder.Encode(time.UTC, colIDs, datums, nil, nil)
	require.NoError(t, err)
	require.True(t, rowcodec.IsNewFormat(encoded))
	return encoded
}

func TestExecuteCachedTaskBasic(t *testing.T) {
	tblInfo := buildTestTableInfo()
	schema := buildTestSchema(tblInfo)
	sctx := mock.NewContext()

	const tableID int64 = 100
	const readTS uint64 = 200

	// Prepare 3 rows: handle=1 val=10, handle=2 val=20, handle=3 val=30.
	type testRow struct {
		handle int64
		val    int64
	}
	rows := []testRow{{1, 10}, {2, 20}, {3, 30}}

	// Build snapshot data.
	snapData := make(map[string][]byte)
	for _, r := range rows {
		key := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(r.handle))
		snapData[string(key)] = encodeTestRow(t, r.val)
	}

	snap := newMockSnapshot(snapData)
	storage := &mockCacheStorage{snapshot: snap}
	cacheDB := newTestCacheDBForSnapshot()

	// Build the row decoder.
	rd := NewRowDecoder(sctx, schema, tblInfo)

	// Build a minimal IndexLookUpExecutor.
	tbl := tables.MockTableFromMeta(tblInfo)
	e := &IndexLookUpExecutor{
		indexLookUpExecutorContext: indexLookUpExecutorContext{
			storage: storage,
		},
		BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), schema, 0),
		table:          tbl,
		startTS:        readTS,
		isCachedTable:  true,
		cacheDB:        cacheDB,
		rowDecoder:     rd,
	}

	// Build task with handles.
	handles := []kv.Handle{kv.IntHandle(1), kv.IntHandle(2), kv.IntHandle(3)}
	task := &lookupTableTask{
		handles: handles,
		idxRows: chunk.New(nil, 0, 0),
	}

	w := &tableWorker{
		idxLookup:  e,
		memTracker: memory.NewTracker(-1, -1),
	}

	// Execute cached task.
	err := w.executeCachedTask(context.Background(), task)
	require.NoError(t, err)

	// Verify results.
	require.Len(t, task.rows, 3)
	for i, r := range rows {
		row := task.rows[i]
		gotID := row.GetInt64(0) // column "id" from handle
		gotVal := row.GetInt64(1) // column "val" from row data
		assert.Equal(t, r.handle, gotID, "row %d id", i)
		assert.Equal(t, r.val, gotVal, "row %d val", i)
	}
	assert.True(t, task.memUsage > 0)
}

func TestExecuteCachedTaskCacheHitOnSecondCall(t *testing.T) {
	tblInfo := buildTestTableInfo()
	schema := buildTestSchema(tblInfo)
	sctx := mock.NewContext()

	const tableID int64 = 100
	const readTS uint64 = 200

	key := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(1))
	snapData := map[string][]byte{
		string(key): encodeTestRow(t, 42),
	}

	snap := newMockSnapshot(snapData)
	storage := &mockCacheStorage{snapshot: snap}
	cacheDB := newTestCacheDBForSnapshot()
	rd := NewRowDecoder(sctx, schema, tblInfo)
	tbl := tables.MockTableFromMeta(tblInfo)

	e := &IndexLookUpExecutor{
		indexLookUpExecutorContext: indexLookUpExecutorContext{
			storage: storage,
		},
		BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), schema, 0),
		table:          tbl,
		startTS:        readTS,
		isCachedTable:  true,
		cacheDB:        cacheDB,
		rowDecoder:     rd,
	}

	// First call: cache miss, fetches from snapshot.
	task1 := &lookupTableTask{
		handles: []kv.Handle{kv.IntHandle(1)},
		idxRows: chunk.New(nil, 0, 0),
	}
	w := &tableWorker{
		idxLookup:  e,
		memTracker: memory.NewTracker(-1, -1),
	}
	err := w.executeCachedTask(context.Background(), task1)
	require.NoError(t, err)
	require.Len(t, task1.rows, 1)
	assert.Equal(t, int64(42), task1.rows[0].GetInt64(1))
	assert.Equal(t, 1, snap.getCount()) // one snapshot fetch

	// Second call: should be served from cache (no new snapshot fetches).
	task2 := &lookupTableTask{
		handles: []kv.Handle{kv.IntHandle(1)},
		idxRows: chunk.New(nil, 0, 0),
	}
	w2 := &tableWorker{
		idxLookup:  e,
		memTracker: memory.NewTracker(-1, -1),
	}
	err = w2.executeCachedTask(context.Background(), task2)
	require.NoError(t, err)
	require.Len(t, task2.rows, 1)
	assert.Equal(t, int64(42), task2.rows[0].GetInt64(1))
	assert.Equal(t, 1, snap.getCount()) // still one - served from cache
}

func TestExecuteCachedTaskMissingHandle(t *testing.T) {
	tblInfo := buildTestTableInfo()
	schema := buildTestSchema(tblInfo)
	sctx := mock.NewContext()

	const tableID int64 = 100
	const readTS uint64 = 200

	// Only handle=1 exists; handle=2 is missing.
	key := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(1))
	snapData := map[string][]byte{
		string(key): encodeTestRow(t, 10),
	}

	snap := newMockSnapshot(snapData)
	storage := &mockCacheStorage{snapshot: snap}
	cacheDB := newTestCacheDBForSnapshot()
	rd := NewRowDecoder(sctx, schema, tblInfo)
	tbl := tables.MockTableFromMeta(tblInfo)

	e := &IndexLookUpExecutor{
		indexLookUpExecutorContext: indexLookUpExecutorContext{
			storage:         storage,
			weakConsistency: true, // skip consistency check for this test
		},
		BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), schema, 0),
		table:          tbl,
		startTS:        readTS,
		isCachedTable:  true,
		cacheDB:        cacheDB,
		rowDecoder:     rd,
	}

	task := &lookupTableTask{
		handles: []kv.Handle{kv.IntHandle(1), kv.IntHandle(2)},
		idxRows: chunk.New(nil, 0, 0),
	}
	w := &tableWorker{
		idxLookup:  e,
		memTracker: memory.NewTracker(-1, -1),
	}

	err := w.executeCachedTask(context.Background(), task)
	require.NoError(t, err)
	// Only 1 row returned (handle=2 doesn't exist).
	require.Len(t, task.rows, 1)
	assert.Equal(t, int64(1), task.rows[0].GetInt64(0))
	assert.Equal(t, int64(10), task.rows[0].GetInt64(1))
}

func TestExecuteCachedTaskKeepOrder(t *testing.T) {
	tblInfo := buildTestTableInfo()
	schema := buildTestSchema(tblInfo)
	sctx := mock.NewContext()

	const tableID int64 = 100
	const readTS uint64 = 200

	// Create rows.
	snapData := make(map[string][]byte)
	for _, h := range []int64{1, 2, 3} {
		key := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(h))
		snapData[string(key)] = encodeTestRow(t, h*10)
	}

	snap := newMockSnapshot(snapData)
	storage := &mockCacheStorage{snapshot: snap}
	cacheDB := newTestCacheDBForSnapshot()
	rd := NewRowDecoder(sctx, schema, tblInfo)
	tbl := tables.MockTableFromMeta(tblInfo)

	// Build a minimal table plan with the schema so that getHandle's
	// needPartitionHandle can inspect the last output column.
	tablePlan := &physicalop.PhysicalTableScan{}
	tablePlan.SetSchema(schema)

	e := &IndexLookUpExecutor{
		indexLookUpExecutorContext: indexLookUpExecutorContext{
			storage: storage,
		},
		BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), schema, 0),
		table:          tbl,
		startTS:        readTS,
		isCachedTable:  true,
		cacheDB:        cacheDB,
		rowDecoder:     rd,
		tblPlans:       []base.PhysicalPlan{tablePlan},
		tableRequest:   &tipb.DAGRequest{OutputOffsets: []uint32{0, 1}},
		index:          &model.IndexInfo{},
		handleCols: []*expression.Column{
			{
				UniqueID: 1,
				ID:       1,
				RetType:  types.NewFieldType(mysql.TypeLonglong),
				Index:    0,
			},
		},
	}

	// Handles in reverse order: 3, 1, 2.
	handles := []kv.Handle{kv.IntHandle(3), kv.IntHandle(1), kv.IntHandle(2)}
	indexOrder := kv.NewHandleMap()
	for i, h := range handles {
		indexOrder.Set(h, i)
	}

	task := &lookupTableTask{
		handles:    handles,
		indexOrder: indexOrder,
		idxRows:    chunk.New(nil, 0, 0),
	}

	w := &tableWorker{
		idxLookup:  e,
		keepOrder:  true,
		handleIdx:  []int{0}, // column 0 is the handle
		memTracker: memory.NewTracker(-1, -1),
	}

	err := w.executeCachedTask(context.Background(), task)
	require.NoError(t, err)
	require.Len(t, task.rows, 3)

	// After keepOrder sort, rows should be in index order: handle 3, 1, 2.
	assert.Equal(t, int64(3), task.rows[0].GetInt64(0))
	assert.Equal(t, int64(30), task.rows[0].GetInt64(1))
	assert.Equal(t, int64(1), task.rows[1].GetInt64(0))
	assert.Equal(t, int64(10), task.rows[1].GetInt64(1))
	assert.Equal(t, int64(2), task.rows[2].GetInt64(0))
	assert.Equal(t, int64(20), task.rows[2].GetInt64(1))
}

func TestExecuteCachedTaskWithInvalidation(t *testing.T) {
	tblInfo := buildTestTableInfo()
	schema := buildTestSchema(tblInfo)
	sctx := mock.NewContext()

	const tableID int64 = 100
	const readTS uint64 = 150

	key := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(1))
	snapData := map[string][]byte{
		string(key): encodeTestRow(t, 42),
	}

	snap := newMockSnapshot(snapData)
	storage := &mockCacheStorage{snapshot: snap}
	cacheDB := newTestCacheDBForSnapshot()
	rd := NewRowDecoder(sctx, schema, tblInfo)
	tbl := tables.MockTableFromMeta(tblInfo)

	e := &IndexLookUpExecutor{
		indexLookUpExecutorContext: indexLookUpExecutorContext{
			storage: storage,
		},
		BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), schema, 0),
		table:          tbl,
		startTS:        readTS,
		isCachedTable:  true,
		cacheDB:        cacheDB,
		rowDecoder:     rd,
	}

	// First call populates cache.
	task1 := &lookupTableTask{
		handles: []kv.Handle{kv.IntHandle(1)},
		idxRows: chunk.New(nil, 0, 0),
	}
	w := &tableWorker{
		idxLookup:  e,
		memTracker: memory.NewTracker(-1, -1),
	}
	err := w.executeCachedTask(context.Background(), task1)
	require.NoError(t, err)
	assert.Equal(t, 1, snap.getCount())

	// Invalidate the key with min_cached_ts=200.
	cacheDB.InvalidateKeys(tableID, []kv.Key{key}, 200)

	// Second call with readTS=150 <= min_cached_ts=200 bypasses cache.
	task2 := &lookupTableTask{
		handles: []kv.Handle{kv.IntHandle(1)},
		idxRows: chunk.New(nil, 0, 0),
	}
	w2 := &tableWorker{
		idxLookup:  e,
		memTracker: memory.NewTracker(-1, -1),
	}
	err = w2.executeCachedTask(context.Background(), task2)
	require.NoError(t, err)
	assert.Equal(t, 2, snap.getCount()) // fetched from snapshot again
	require.Len(t, task2.rows, 1)
	assert.Equal(t, int64(42), task2.rows[0].GetInt64(1))
}
