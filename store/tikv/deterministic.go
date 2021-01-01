// Copyright 2020 PingCAP, Inc.
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

package tikv

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// deterministicCommitter executes a deterministic commit protocol.
type deterministicCommitter struct {
	store     *tikvStore
	txn       *tikvTxn
	startTS   uint64
	mutations *memBufferMutations
	// commitTS in deterministic transaction means
	commitTS uint64
	priority pb.CommandPri
	connID   uint64 // connID is used for log.
	cleanWg  sync.WaitGroup
	detail   unsafe.Pointer
	txnSize  int

	mu struct {
		sync.RWMutex
		undeterminedErr error // undeterminedErr saves the rpc error we encounter when commit primary key.
		committed       bool
	}
	// regionTxnSize stores the number of keys involved in each region
	regionTxnSize map[uint64]int
}

func newDeterministicCommitter(txn *tikvTxn, connID uint64) (*deterministicCommitter, error) {
	return &deterministicCommitter{
		store:         txn.store,
		txn:           txn,
		startTS:       txn.StartTS(),
		connID:        connID,
		regionTxnSize: map[uint64]int{},
	}, nil
}

func (c *deterministicCommitter) GetStartTS() uint64 {
	return c.startTS
}

func (c *deterministicCommitter) GetCommitTS() uint64 {
	return c.commitTS
}

func (c *deterministicCommitter) GetMutations() *memBufferMutations {
	return c.mutations
}

func (c *deterministicCommitter) GetTtlManager() *ttlManager {
	panic("unreachable")
}

func (c *deterministicCommitter) GetCleanWg() *sync.WaitGroup {
	return &c.cleanWg
}

func (c *deterministicCommitter) getDetail() *execdetails.CommitDetails {
	return (*execdetails.CommitDetails)(atomic.LoadPointer(&c.detail))
}

func (c *deterministicCommitter) initKeysAndMutations() error {
	var size, putCnt, delCnt, lockCnt, checkCnt int

	txn := c.txn
	memBuf := txn.GetMemBuffer()
	sizeHint := txn.us.GetMemBuffer().Len()
	c.mutations = newMemBufferMutations(sizeHint, memBuf)

	var err error
	for it := memBuf.IterWithFlags(nil, nil); it.Valid(); err = it.Next() {
		_ = err
		key := it.Key()
		flags := it.Flags()
		var value []byte
		var op pb.Op

		if !it.HasValue() {
			if !flags.HasLocked() {
				continue
			}
			op = pb.Op_Lock
			lockCnt++
		} else {
			value = it.Value()
			if len(value) > 0 {
				if tablecodec.IsUntouchedIndexKValue(key, value) {
					if !flags.HasLocked() {
						continue
					}
					op = pb.Op_Lock
					lockCnt++
				} else {
					op = pb.Op_Put
					if flags.HasPresumeKeyNotExists() {
						op = pb.Op_Insert
					}
					putCnt++
				}
			} else {
				if !txn.IsPessimistic() && flags.HasPresumeKeyNotExists() {
					// delete-your-writes keys in optimistic txn need check not exists in prewrite-phase
					// due to `Op_CheckNotExists` doesn't prewrite lock, so mark those keys should not be used in commit-phase.
					op = pb.Op_CheckNotExists
					checkCnt++
					memBuf.UpdateFlags(key, kv.SetPrewriteOnly)
				} else {
					// normal delete keys in optimistic txn can be delete without not exists checking
					// delete-your-writes keys in pessimistic txn can ensure must be no exists so can directly delete them
					op = pb.Op_Del
					delCnt++
				}
			}
		}

		c.mutations.Push(op, false, it.Handle())
		size += len(key) + len(value)
	}

	if c.mutations.Len() == 0 {
		return nil
	}
	c.txnSize = size

	if size > int(kv.TxnTotalSizeLimit) {
		return kv.ErrTxnTooLarge.GenWithStackByArgs(size)
	}
	const logEntryCount = 10000
	const logSize = 4 * 1024 * 1024 // 4MB
	if c.mutations.Len() > logEntryCount || size > logSize {
		tableID := tablecodec.DecodeTableID(c.mutations.GetKey(0))
		logutil.BgLogger().Info("[BIG_TXN]",
			zap.Uint64("con", c.connID),
			zap.Int64("table ID", tableID),
			zap.Int("size", size),
			zap.Int("keys", c.mutations.Len()),
			zap.Int("puts", putCnt),
			zap.Int("dels", delCnt),
			zap.Int("locks", lockCnt),
			zap.Int("checks", checkCnt),
			zap.Uint64("txnStartTS", txn.startTS))
	}

	// Sanity check for startTS.
	if txn.StartTS() == math.MaxUint64 {
		err = errors.Errorf("try to commit with invalid txnStartTS: %d", txn.StartTS())
		logutil.BgLogger().Error("commit failed",
			zap.Uint64("conn", c.connID),
			zap.Error(err))
		return errors.Trace(err)
	}

	commitDetail := &execdetails.CommitDetails{WriteSize: size, WriteKeys: c.mutations.Len()}
	metrics.TiKVTxnWriteKVCountHistogram.Observe(float64(commitDetail.WriteKeys))
	metrics.TiKVTxnWriteSizeHistogram.Observe(float64(commitDetail.WriteSize))
	c.priority = getTxnPriority(txn)
	c.setDetail(commitDetail)
	return nil
}

func (c *deterministicCommitter) isAsyncCommit() bool {
	return false
}

func (c *deterministicCommitter) setDetail(d *execdetails.CommitDetails) {
	atomic.StorePointer(&c.detail, unsafe.Pointer(d))
}

func (c *deterministicCommitter) pessimisticRollbackMutations(bo *Backoffer, mutations CommitterMutations) error {
	return nil
}

func (c *deterministicCommitter) extractKeyExistsErr(key kv.Key) error {
	return nil
}

func (c *deterministicCommitter) cleanup(ctx context.Context) {
	c.cleanWg.Add(1)
	go func() {
		c.cleanWg.Done()
	}()
}

func (c *deterministicCommitter) execute(context.Context) error {
	return nil
}
