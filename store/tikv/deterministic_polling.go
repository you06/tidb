package tikv

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/store/tikv/oracle"
)

type batchManagerPolling struct {
	sync.Mutex
	bmArr      []*batchManager
	startTSArr []uint64
	index      int32
	count      int32
}

func newBatchManagerPolling(store *tikvStore, count int) (*batchManagerPolling, error) {
	var (
		bmArr      = make([]*batchManager, count)
		startTSArr = make([]uint64, count)
	)

	for i := 0; i < count; i++ {
		bmArr[i], _ = newBatchManager(store)
	}

	return &batchManagerPolling{
		bmArr:      bmArr,
		startTSArr: startTSArr,
		index:      0,
		count:      int32(count),
	}, nil
}

func (b *batchManagerPolling) NextBatch(ctx context.Context) oracle.Future {
	var (
		i      int32
		next   int32
		future oracle.Future
	)
WAIT:
	i = atomic.LoadInt32(&b.index)
	future = b.bmArr[i].NextBatch(ctx)
	if future != nil {
		return future
	}
	if i == b.index-1 {
		next = 0
	} else {
		next = i + 1
	}
	if atomic.CompareAndSwapInt32(&b.index, i, next) {
		goto WAIT
	} else {
		goto WAIT
	}
	return nil
}

func (b *batchManagerPolling) GetByStartTS(ts uint64) *batchManager {
	for i := 0; i < int(b.count); i++ {
		if b.bmArr[i].startTS == ts {
			return b.bmArr[i]
		}
	}
	return nil
}
