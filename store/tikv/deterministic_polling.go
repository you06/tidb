package tikv

import (
	"context"
	"sync"

	"github.com/pingcap/tidb/store/tikv/oracle"
)

const (
	DeterministicPoolSize            = 4
	DeterministicMaxBatchSize uint32 = 128
)

type batchManagerPolling struct {
	sync.Mutex
	rwlock      sync.RWMutex
	store       *tikvStore
	currManager *batchManager
	bms         map[uint64]*batchManager
	batchStatus uint32
	round       int
}

func newBatchManagerPolling(store *tikvStore, count int) (*batchManagerPolling, error) {
	return &batchManagerPolling{
		store: store,
		bms:   make(map[uint64]*batchManager),
	}, nil
}

func (b *batchManagerPolling) NextBatch(ctx context.Context) oracle.Future {
	b.Lock()
	b.batchStatus++
	if b.batchStatus == 1 {
		b.currManager, _ = newBatchManager(b.store, b, b.currManager)
	}
	future := b.currManager.NextBatch(ctx)
	if b.batchStatus == DeterministicMaxBatchSize {
		go b.currManager.writeCheckpointStart()
		b.batchStatus = 0
		b.round++
	}
	b.Unlock()
	return future
}

func (b *batchManagerPolling) SetManager(ts uint64, bm *batchManager) {
	b.rwlock.Lock()
	b.bms[ts] = bm
	b.rwlock.Unlock()
}

func (b *batchManagerPolling) DelManager(ts uint64) {
	b.rwlock.Lock()
	delete(b.bms, ts)
	b.rwlock.Unlock()
}

func (b *batchManagerPolling) GetByStartTS(ts uint64) *batchManager {
	b.rwlock.RLock()
	bm := b.bms[ts]
	b.rwlock.RUnlock()
	return bm
}
