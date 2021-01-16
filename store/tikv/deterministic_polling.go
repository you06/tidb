package tikv

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/store/tikv/oracle"
)

const (
	DeterministicPoolSize            = 4
	DeterministicMaxBatchSize uint32 = 128
	FlushDuration                    = 50 * time.Millisecond
)

type batchManagerPolling struct {
	sync.Mutex
	rwlock      sync.RWMutex
	store       *tikvStore
	currManager *batchManager
	bms         map[uint64]*batchManager
	batchStatus uint32
	round       int

	pessimisticMu          sync.Mutex
	pessimisticManager     *batchManager
	pessimisticBatchStatus uint32
	pessimisticRound       int
}

func newBatchManagerPolling(store *tikvStore, count int) (*batchManagerPolling, error) {
	p := &batchManagerPolling{
		store: store,
		bms:   make(map[uint64]*batchManager),
	}
	go p.CheckFlush()
	return p, nil
}

func (b *batchManagerPolling) CheckFlush() {
	var (
		beforeBatchStatus            uint32
		beforeRound                  int
		beforePessimisticBatchStatus uint32
		beforePessimisticRound       int
		ticker                       = time.NewTicker(FlushDuration)
	)
	for {
		<-ticker.C
		b.Lock()
		if b.batchStatus > 0 && b.batchStatus == beforeBatchStatus && b.round == beforeRound {
			b.batchStatus = 0
			b.round++
			go b.currManager.writeCheckpointStart()
		}
		beforeBatchStatus = b.batchStatus
		beforeRound = b.round
		b.Unlock()

		b.pessimisticMu.Lock()
		if b.pessimisticBatchStatus > 0 &&
			b.pessimisticBatchStatus == beforePessimisticBatchStatus &&
			b.pessimisticRound == beforePessimisticRound {
			b.pessimisticBatchStatus = 0
			b.round++
			go b.pessimisticManager.commitDirectly()
		}
		beforePessimisticBatchStatus = b.pessimisticBatchStatus
		beforePessimisticRound = b.pessimisticRound
		b.pessimisticMu.Unlock()
	}
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

func (b *batchManagerPolling) GetPessimisticBatch(txn *tikvTxn) *batchManager {
	b.pessimisticMu.Lock()
	b.pessimisticBatchStatus++
	if b.pessimisticBatchStatus == 1 {
		b.pessimisticManager, _ = newBatchManager(b.store, b, nil)
		b.pessimisticManager.SkipLock = false
		b.pessimisticManager.IsPessimisticBatch = true
	}
	bm := b.pessimisticManager
	bm.futureCount++
	bm.txns[txn] = struct{}{}
	if b.pessimisticBatchStatus == DeterministicMaxBatchSize {
		go b.pessimisticManager.commitDirectly()
		b.pessimisticBatchStatus = 0
		b.pessimisticRound++
	}
	b.pessimisticMu.Unlock()
	return bm
}

func (b *batchManagerPolling) SetManager(ts uint64, bm *batchManager) {
	b.rwlock.Lock()
	b.bms[ts] = bm
	b.rwlock.Unlock()
}

func (b *batchManagerPolling) DelManager(ts uint64) {
	b.rwlock.Lock()
	if _, ok := b.bms[ts]; !ok {
		logutil.BgLogger().Info("MYLOG DelManager bm not found", zap.Uint64("ts", ts))
	}
	delete(b.bms, ts)
	b.rwlock.Unlock()
}

func (b *batchManagerPolling) GetByStartTS(ts uint64) *batchManager {
	b.rwlock.RLock()
	bm := b.bms[ts]
	b.rwlock.RUnlock()
	return bm
}
