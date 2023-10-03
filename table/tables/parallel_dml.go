// Copyright 2023 PingCAP, Inc.
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

package tables

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/tracing"
	"go.uber.org/zap"
)

// ParallelTable eliminates the mem buffer of TableCommon can write concurrently with safety guarantee.
// The AddRecord method of TableCommon is override by ParallelTable to be a thread-safe one.
// For the atomicity of transactions, the ParallelTable also need to follow the 2 phase-commit protocol.
// When executor adds records to the table with ParallelTable, the prewrite protocol is used.
type ParallelTable struct {
	*TableCommon
	sessVars    *variable.SessionVars
	txn         kv.Transaction
	tableCached atomic.Bool
}

func NewParallelWriter(sctx sessionctx.Context, tbl *TableCommon, concurrency int) (*ParallelTable, error) {
	if m := tbl.Meta(); m.TempTableType == model.TempTableNone {
		return nil, errors.New("ParallelTable does not support temporary table")
	}
	txn, err := sctx.Txn(true)
	if err != nil {
		return nil, err
	}
	txn.ParallelInitWriter(concurrency)
	sessVars := sctx.GetSessionVars()
	return &ParallelTable{
		TableCommon: tbl,
		sessVars:    sessVars,
		txn:         txn,
	}, nil
}

// AddRecord implements table.Table AddRecord interface.
// This function is thread-safe.
func (p *ParallelTable) AddRecord(sctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	var opt table.AddRecordOpt
	for _, fn := range opts {
		fn.ApplyOn(&opt)
	}

	var ctx context.Context
	if opt.Ctx != nil {
		ctx = opt.Ctx
		var r tracing.Region
		r, ctx = tracing.StartRegionEx(ctx, "table.AddRecord")
		defer r.End()
	} else {
		ctx = context.Background()
	}
	var hasRecordID bool
	cols := p.Cols()
	// opt.IsUpdate is a flag for update.
	// If handle ID is changed when update, update will remove the old record first, and then call `AddRecord` to add a new record.
	// Currently, only insert can set _tidb_rowid, update can not update _tidb_rowid.
	if len(r) > len(cols) && !opt.IsUpdate {
		// The last value is _tidb_rowid.
		recordID = kv.IntHandle(r[len(r)-1].GetInt64())
		hasRecordID = true
	} else {
		tblInfo := p.Meta()
		if p.tableCached.CompareAndSwap(false, true) {
			p.txn.CacheTableInfo(p.physicalTableID, tblInfo)
		}
		if tblInfo.PKIsHandle {
			recordID = kv.IntHandle(r[tblInfo.GetPkColInfo().Offset].GetInt64())
			hasRecordID = true
		} else if tblInfo.IsCommonHandle {
			pkIdx := FindPrimaryIndex(tblInfo)
			pkDts := make([]types.Datum, 0, len(pkIdx.Columns))
			for _, idxCol := range pkIdx.Columns {
				pkDts = append(pkDts, r[idxCol.Offset])
			}
			tablecodec.TruncateIndexValues(tblInfo, pkIdx, pkDts)
			var handleBytes []byte
			handleBytes, err = codec.EncodeKey(sctx.GetSessionVars().StmtCtx, nil, pkDts...)
			if err != nil {
				return
			}
			recordID, err = kv.NewCommonHandle(handleBytes)
			if err != nil {
				return
			}
			hasRecordID = true
		}
	}
	if !hasRecordID {
		if opt.ReserveAutoID > 0 {
			// Reserve a batch of auto ID in the statement context.
			// The reserved ID could be used in the future within this statement, by the
			// following AddRecord() operation.
			// Make the IDs continuous benefit for the performance of TiKV.
			stmtCtx := sctx.GetSessionVars().StmtCtx
			stmtCtx.BaseRowID, stmtCtx.MaxRowID, err = allocHandleIDs(ctx, sctx, p, uint64(opt.ReserveAutoID))
			if err != nil {
				return nil, err
			}
		}

		// TODO(you06): optimize the handle allocation, there may be lock contention.
		recordID, err = AllocHandle(ctx, sctx, p)
		if err != nil {
			return nil, err
		}
	}

	var colIDs []int64
	var row []types.Datum
	var checksums []uint32
	if recordCtx, ok := sctx.Value(addRecordCtxKey).(*CommonAddRecordCtx); ok {
		colIDs = recordCtx.colIDs[:0]
		row = recordCtx.row[:0]
	} else {
		// TODO(you06): reuse the colIDs and row.
		colIDs = make([]int64, 0, len(r))
		row = make([]types.Datum, 0, len(r))
	}

	sessVars := p.sessVars
	checksumData := p.initChecksumData(sctx, recordID)
	needChecksum := len(checksumData) > 0

	for _, col := range p.Columns {
		var value types.Datum
		if col.State == model.StateDeleteOnly || col.State == model.StateDeleteReorganization {
			if needChecksum {
				if col.ChangeStateInfo != nil {
					// TODO: Check overflow or ignoreTruncate.
					v, err := table.CastValue(sctx, r[col.DependencyColumnOffset], col.ColumnInfo, false, false)
					if err != nil {
						return nil, err
					}
					checksumData = p.appendInChangeColForChecksum(sctx, recordID, checksumData, col.ToInfo(), &r[col.DependencyColumnOffset], &v)
				} else {
					v, err := table.GetColOriginDefaultValue(sctx, col.ToInfo())
					if err != nil {
						return nil, err
					}
					checksumData = p.appendNonPublicColForChecksum(sctx, recordID, checksumData, col.ToInfo(), &v)
				}
			}
			continue
		}
		// In column type change, since we have set the origin default value for changing col, but
		// for the new insert statement, we should use the casted value of relative column to insert.
		if col.ChangeStateInfo != nil && col.State != model.StatePublic {
			// TODO: Check overflow or ignoreTruncate.
			value, err = table.CastValue(sctx, r[col.DependencyColumnOffset], col.ColumnInfo, false, false)
			if err != nil {
				return nil, err
			}
			if len(r) < len(p.WritableCols()) {
				r = append(r, value)
			} else {
				r[col.Offset] = value
			}
			row = append(row, value)
			colIDs = append(colIDs, col.ID)
			checksumData = p.appendInChangeColForChecksum(sctx, recordID, checksumData, col.ToInfo(), &r[col.DependencyColumnOffset], &value)
			continue
		}
		if col.State == model.StatePublic {
			value = r[col.Offset]
			checksumData = p.appendPublicColForChecksum(sctx, recordID, checksumData, col.ToInfo(), &value)
		} else {
			// col.ChangeStateInfo must be nil here.
			// because `col.State != model.StatePublic` is true here, if col.ChangeStateInfo is not nil, the col should
			// be handle by the previous if-block.

			if opt.IsUpdate {
				// If `AddRecord` is called by an update, the default value should be handled the update.
				value = r[col.Offset]
			} else {
				// If `AddRecord` is called by an insert and the col is in write only or write reorganization state, we must
				// add it with its default value.
				value, err = table.GetColOriginDefaultValue(sctx, col.ToInfo())
				if err != nil {
					return nil, err
				}
				// add value to `r` for dirty db in transaction.
				// Otherwise when update will panic cause by get value of column in write only state from dirty db.
				if col.Offset < len(r) {
					r[col.Offset] = value
				} else {
					r = append(r, value)
				}
			}
			checksumData = p.appendNonPublicColForChecksum(sctx, recordID, checksumData, col.ToInfo(), &value)
		}
		if !p.canSkip(col, &value) {
			colIDs = append(colIDs, col.ID)
			row = append(row, value)
		}
	}
	// check data constraint
	err = p.CheckRowConstraint(sctx, r)
	if err != nil {
		return nil, err
	}

	writeBufs := opt.LocalVars.WriteBufs
	adjustRowValuesBuf(writeBufs, len(row))
	key := p.RecordKey(recordID)
	logutil.BgLogger().Debug("addRecord",
		zap.Stringer("key", key))
	sc, rd := sessVars.StmtCtx, &sessVars.RowEncoder
	checksums, writeBufs.RowValBuf = p.calcChecksums(sctx, recordID, checksumData, writeBufs.RowValBuf)
	writeBufs.RowValBuf, err = tablecodec.EncodeRow(sc, row, colIDs, writeBufs.RowValBuf, writeBufs.AddRowValues, rd, checksums...)
	if err != nil {
		return nil, err
	}
	value := writeBufs.RowValBuf

	err = p.txn.ParallelSet(opt.LocalVars.N, key, value)
	if err != nil {
		return nil, err
	}

	var createIdxOpts []table.CreateIdxOptFunc
	if len(opts) > 0 {
		createIdxOpts = make([]table.CreateIdxOptFunc, 0, len(opts))
		for _, fn := range opts {
			if raw, ok := fn.(table.CreateIdxOptFunc); ok {
				createIdxOpts = append(createIdxOpts, raw)
			}
		}
	}
	// Insert new entries into indices.
	h, err := p.addIndices(sctx, recordID, r, createIdxOpts, opt.LocalVars)
	if err != nil {
		return h, err
	}

	if sessVars.TxnCtx == nil {
		return recordID, nil
	}

	if shouldIncreaseTTLMetricCount(p.meta) {
		opt.LocalVars.InsertTTLRowsCount += 1
	}

	colSize := make(map[int64]int64, len(r))
	for id, col := range p.Cols() {
		size, err := codec.EstimateValueSize(sc, r[id])
		if err != nil {
			continue
		}
		colSize[col.ID] = int64(size) - 1
	}
	// TODO(you06): store the delta into local variables to make it compatible with DDL.
	//sessVars.TxnCtx.UpdateDeltaForTable(p.physicalTableID, 1, 1, colSize)
	return recordID, nil
}

// addIndices adds data into indices. If any key is duplicated, returns the original handle.
func (p *ParallelTable) addIndices(sctx sessionctx.Context, recordID kv.Handle, r []types.Datum, opts []table.CreateIdxOptFunc, local *table.LocalVars) (kv.Handle, error) {
	writeBufs := local.WriteBufs
	indexVals := writeBufs.IndexValsBuf
	skipCheck := sctx.GetSessionVars().StmtCtx.BatchCheck
	for _, v := range p.Indices() {
		if !IsIndexWritable(v) {
			continue
		}
		if p.meta.IsCommonHandle && v.Meta().Primary {
			continue
		}
		indexVals, err := v.FetchValues(r, indexVals)
		if err != nil {
			return nil, err
		}
		var dupErr error
		if !skipCheck && v.Meta().Unique {
			entryKey, err := genIndexKeyStr(indexVals)
			if err != nil {
				return nil, err
			}
			dupErr = kv.ErrKeyExists.FastGenByArgs(entryKey, fmt.Sprintf("%s.%s", v.TableMeta().Name.String(), v.Meta().Name.String()))
		}
		rsData := TryGetHandleRestoredDataWrapper(p.meta, r, nil, v.Meta())
		if dupHandle, err := v.Create(sctx, p.txn, indexVals, recordID, rsData, opts...); err != nil {
			if kv.ErrKeyExists.Equal(err) {
				return dupHandle, dupErr
			}
			return nil, err
		}
	}
	// save the buffer, multi rows insert can use it.
	writeBufs.IndexValsBuf = indexVals
	return nil, nil
}

func (p *ParallelTable) createIndex(c *index, local *table.LocalVars, indexedValue []types.Datum, h kv.Handle, handleRestoreData []types.Datum, opts ...table.CreateIdxOptFunc) (kv.Handle, error) {
	//if p.Meta().Unique {
	//	txn.CacheTableInfo(c.phyTblID, c.tblInfo)
	//}
	var opt table.CreateIdxOpt
	for _, fn := range opts {
		fn(&opt)
	}

	indexedValues := c.getIndexedValue(indexedValue)
	ctx := opt.Ctx
	if ctx != nil {
		var r tracing.Region
		r, ctx = tracing.StartRegionEx(ctx, "index.Create")
		defer r.End()
	} else {
		ctx = context.TODO()
	}
	writeBufs := local.WriteBufs
	skipCheck := true
	for _, value := range indexedValues {
		key, distinct, err := c.GenIndexKey(p.sessVars.StmtCtx, value, h, writeBufs.IndexKeyBuf)
		if err != nil {
			return nil, err
		}

		var (
			tempKey         []byte
			keyVer          byte
			keyIsTempIdxKey bool
		)
		if !opt.FromBackFill {
			key, tempKey, keyVer = GenTempIdxKeyByState(c.idxInfo, key)
			if keyVer == TempIndexKeyTypeBackfill || keyVer == TempIndexKeyTypeDelete {
				key, tempKey = tempKey, nil
				keyIsTempIdxKey = true
			}
		}

		// save the key buffer to reuse.
		writeBufs.IndexKeyBuf = key
		c.initNeedRestoreData.Do(func() {
			c.needRestoredData = NeedRestoredData(c.idxInfo.Columns, c.tblInfo.Columns)
		})
		idxVal, err := tablecodec.GenIndexValuePortal(p.sessVars.StmtCtx, c.tblInfo, c.idxInfo, c.needRestoredData, distinct, opt.Untouched, value, h, c.phyTblID, handleRestoreData)
		if err != nil {
			return nil, err
		}

		opt.IgnoreAssertion = opt.IgnoreAssertion || c.idxInfo.State != model.StatePublic

		if !distinct || skipCheck || opt.Untouched {
			val := idxVal
			if opt.Untouched && (keyIsTempIdxKey || len(tempKey) > 0) {
				// Untouched key-values never occur in the storage and the temp index is not public.
				// It is unnecessary to write the untouched temp index key-values.
				continue
			}
			if keyIsTempIdxKey {
				tempVal := tablecodec.TempIndexValueElem{Value: idxVal, KeyVer: keyVer, Distinct: distinct}
				val = tempVal.Encode(nil)
			}
			err = p.txn.ParallelSet(local.N, key, val)
			if err != nil {
				return nil, err
			}
			if len(tempKey) > 0 {
				tempVal := tablecodec.TempIndexValueElem{Value: idxVal, KeyVer: keyVer, Distinct: distinct}
				val = tempVal.Encode(nil)
				err = p.txn.ParallelSet(local.N, tempKey, val)
				if err != nil {
					return nil, err
				}
			}
			//if !opt.IgnoreAssertion && (!opt.Untouched) {
			//	if sctx.GetSessionVars().LazyCheckKeyNotExists() && !txn.IsPessimistic() {
			//		err = txn.SetAssertion(key, kv.SetAssertUnknown)
			//	} else {
			//		err = txn.SetAssertion(key, kv.SetAssertNotExist)
			//	}
			//}
			if err != nil {
				return nil, err
			}
			continue
		}

		value, err := p.txn.Get(ctx, key)
		if err != nil && !kv.IsErrNotFound(err) {
			return nil, err
		}
		var tempIdxVal tablecodec.TempIndexValue
		if len(value) > 0 && keyIsTempIdxKey {
			tempIdxVal, err = tablecodec.DecodeTempIndexValue(value)
			if err != nil {
				return nil, err
			}
		}
		// The index key value is not found or deleted.
		if err != nil || len(value) == 0 || (!tempIdxVal.IsEmpty() && tempIdxVal.Current().Delete) {
			val := idxVal
			if keyIsTempIdxKey {
				tempVal := tablecodec.TempIndexValueElem{Value: idxVal, KeyVer: keyVer, Distinct: true}
				val = tempVal.Encode(value)
			}
			//needPresumeNotExists, err := needPresumeKeyNotExistsFlag(ctx, txn, key, tempKey, h,
			//	keyIsTempIdxKey, c.tblInfo.IsCommonHandle, c.tblInfo.ID)
			if err != nil {
				return nil, err
			}
			//var flags []kv.FlagsOp
			//if needPresumeNotExists {
			//	flags = []kv.FlagsOp{kv.SetPresumeKeyNotExists}
			//}
			//if !vars.ConstraintCheckInPlacePessimistic && vars.TxnCtx.IsPessimistic && vars.InTxn() &&
			//	!vars.InRestrictedSQL && vars.ConnectionID > 0 {
			//	flags = append(flags, kv.SetNeedConstraintCheckInPrewrite)
			//}
			//err = txn.GetMemBuffer().SetWithFlags(key, val, flags...)
			err = p.txn.ParallelSet(local.N, key, val)

			if err != nil {
				return nil, err
			}
			if len(tempKey) > 0 {
				tempVal := tablecodec.TempIndexValueElem{Value: idxVal, KeyVer: keyVer, Distinct: true}
				val = tempVal.Encode(value)
				//if lazyCheck && needPresumeNotExists {
				//	err = txn.GetMemBuffer().SetWithFlags(tempKey, val, kv.SetPresumeKeyNotExists)
				//} else {
				//	err = txn.GetMemBuffer().Set(tempKey, val)
				//}
				p.txn.ParallelSet(local.N, tempKey, val)
				if err != nil {
					return nil, err
				}
			}
			if opt.IgnoreAssertion {
				continue
			}
			//if lazyCheck && !txn.IsPessimistic() {
			//	err = txn.SetAssertion(key, kv.SetAssertUnknown)
			//} else {
			//	err = txn.SetAssertion(key, kv.SetAssertNotExist)
			//}
			//if err != nil {
			//	return nil, err
			//}
			continue
		}
		if c.idxInfo.Global && len(value) != 0 && !bytes.Equal(value, idxVal) {
			val := idxVal
			//err = txn.GetMemBuffer().Set(key, val)
			err = p.txn.ParallelSet(local.N, key, val)
			if err != nil {
				return nil, err
			}
			continue
		}

		if keyIsTempIdxKey && !tempIdxVal.IsEmpty() {
			value = tempIdxVal.Current().Value
		}
		handle, err := tablecodec.DecodeHandleInUniqueIndexValue(value, c.tblInfo.IsCommonHandle)
		if err != nil {
			return nil, err
		}
		// unreachable in common path.
		return handle, kv.ErrKeyExists
	}
	return nil, nil
}
