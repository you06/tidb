package tikv

import "context"

type batchManager struct {
	store *tikvStore
	txns  []*tikvTxn

	startTS  uint64
	commitTS uint64
}

var (
	batchSizeHint = 128
)

func newBatchManager(store *tikvStore) (*batchManager, error) {
	return &batchManager{
		store: store,
		txns:  make([]*tikvTxn, 0, batchSizeHint),
	}, nil
}

func (b *batchManager) addTxn(txn *tikvTxn) {
	b.txns = append(b.txns, txn)
}

func (b *batchManager) detectConflicts() []*tikvTxn {
	return nil
}

// NextBatch return next batch's startTS when it's ready
func (b *batchManager) NextBatch(ctx context.Context) (uint64, error) {
	return 0, nil
}
