package tikv

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/store/tikv/tikvrpc"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"

	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"

	"github.com/pingcap/errors"

	"github.com/pingcap/tidb/kv"
)

const (
	CheckPointKey = "checkpoint"

	batchStateFree uint32 = iota
	batchStateStarting
	batchStateProgress

	txnStateInit uint64 = iota
	txnStateBeforeCommit
)

type txnState struct {
	state uint64
	txn   *tikvTxn
}

type batchManager struct {
	state    uint32
	txnCount uint32
	startTS  uint64
	commitTS uint64

	mu           sync.RWMutex
	freeReady    sync.WaitGroup
	startReady   sync.WaitGroup
	commitDone   sync.WaitGroup
	detectDone   sync.WaitGroup
	mutations    *memBufferMutations
	commitErr    error
	store        *tikvStore
	vars         *kv.Variables
	txns         map[uint32]*tikvTxn
	conflictTxns map[uint32]*tikvTxn
}

func newBatchManager(store *tikvStore) (*batchManager, error) {
	return &batchManager{
		store: store,
		txns:  make(map[uint32]*tikvTxn),
		vars:  kv.DefaultVars,
	}, nil
}

// NextBatch return next batch's startTS when it's ready
func (b *batchManager) NextBatch(ctx context.Context, txnScope string) (kv.Transaction, error) {
	b.commitDone.Wait()
	b.freeReady.Wait()
	b.mu.Lock()
	if b.state == batchStateFree {
		b.state = batchStateStarting
		b.startReady.Add(1)
		// allocate a checkpoint for first txn in a batch
		b.writeCheckpointStart()
	}
	b.mu.Unlock()

	b.startReady.Wait()
	txn, err := newTiKVTxnWithStartTS(b.store, txnScope, b.startTS, b.store.nextReplicaReadSeed())
	if err != nil {
		return nil, errors.Trace(err)
	}
	b.addTxn(txn)

	return txn, nil
}

func (b *batchManager) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = batchStateFree
	b.txnCount = 0
	b.state = 0
	b.commitTS = 0
	b.txns = make(map[uint32]*tikvTxn)
}

func (b *batchManager) begin() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.startReady.Add(1)
}

func (b *batchManager) addTxn(txn *tikvTxn) {
	b.mu.Lock()
	b.mu.Unlock()
	if b.startTS < txn.startTS {
		panic("unreachable")
	}
	b.txns[txn.snapshot.replicaReadSeed] = txn
}

func (b *batchManager) removeTxn(txn *tikvTxn) {
	b.mu.Lock()
	b.mu.Unlock()
	if _, ok := b.txns[txn.snapshot.replicaReadSeed]; !ok {
		panic("unreachable")
	}
	delete(b.txns, txn.snapshot.replicaReadSeed)
	if int(b.txnCount) == len(b.txns) {
		b.detectConflicts()
	}
}

func (b *batchManager) mutationReady() {
	b.mu.Lock()
	b.mu.Unlock()
	b.txnCount++
	if int(b.txnCount) == len(b.txns) {
		b.detectConflicts()
	}
}

func (b *batchManager) detectConflicts() {
	var conflictTxns []*tikvTxn
	// implement detection
	for _, txn := range conflictTxns {
		delete(b.txns, txn.snapshot.replicaReadSeed)
	}
	b.detectDone.Done()
	b.commitDone.Add(len(b.txns) - 1)
	b.commitErr = b.commit()
}

func (b *batchManager) hasConflict(txn *tikvTxn) bool {
	b.detectDone.Wait()
	_, ok := b.txns[txn.snapshot.replicaReadSeed]
	return !ok
}

func (b *batchManager) getCommitErr() error {
	b.freeReady.Wait()
	defer b.commitDone.Done()
	return b.commitErr
}

// TODO: handle PD server timeout
func (b *batchManager) writeCheckpointStart() {
	// write checkpointStart
	b.freeReady.Add(1)
	b.detectDone.Add(1)
	b.commitDone.Add(1)
	b.startReady.Done()
}

func (b *batchManager) writeCheckpointCommit() error {

	b.freeReady.Done()
	return nil
}

func (b *batchManager) writeCheckpointRollback() error {

	b.freeReady.Done()
	return nil
}

func (b *batchManager) mergeMutations() {
	for _, txn := range b.txns {
		mutation := txn.committer.GetMutations()
		var (
			op  pb.Op
			key kv.Key
		)
		for i := 0; i < mutation.Len(); i++ {
			op = mutation.GetOp(i)
			// ignore pessimistic
			if op == pb.Op_Lock {
				continue
			}
			key = mutation.GetKey(i)
			handle := txn.GetMemBuffer().IterWithFlags(key, nil).Handle()
			b.mutations.Push(op, false, handle)
		}
	}
}

// groupMutations groups mutations by region, then checks for any large groups and in that case pre-splits the region.
func (b *batchManager) groupMutations(bo *Backoffer, mutations CommitterMutations) ([]groupedMutations, error) {
	groups, err := b.store.regionCache.groupSortedMutationsByRegion(bo, mutations)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Pre-split regions to avoid too much write workload into a single region.
	// In the large transaction case, this operation is important to avoid TiKV 'server is busy' error.
	var didPreSplit bool
	preSplitDetectThresholdVal := atomic.LoadUint32(&preSplitDetectThreshold)
	for _, group := range groups {
		if uint32(group.mutations.Len()) >= preSplitDetectThresholdVal {
			logutil.BgLogger().Info("2PC detect large amount of mutations on a single region",
				zap.Uint64("region", group.region.GetID()),
				zap.Int("mutations count", group.mutations.Len()))
			// Use context.Background, this time should not add up to Backoffer.
			if b.store.preSplitRegion(context.Background(), group) {
				didPreSplit = true
			}
		}
	}
	// Reload region cache again.
	if didPreSplit {
		groups, err = b.store.regionCache.groupSortedMutationsByRegion(bo, mutations)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return groups, nil
}

func (b *batchManager) commit() error {
	var err error
	b.mergeMutations()
	bo := NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, b.vars)
	err = b.writeDeterministicGroups(bo, b.mutations)

	return err
}

func (b *batchManager) writeDeterministicGroups(bo *Backoffer, mutations CommitterMutations) error {
	var (
		groups  []groupedMutations
		groupWg sync.WaitGroup
		err     error
	)
	groups, err = b.groupMutations(bo, mutations)
	if err != nil {
		return err
	}

	groupWg.Add(len(groups))
	for _, group := range groups {
		go func(group groupedMutations) {
			writeBo := NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, b.vars)
			b.writeDeterministic(writeBo, &groupWg, group)
		}(group)
	}
	groupWg.Wait()

	b.writeCheckpointCommit()

	return nil
}

func (b *batchManager) writeDeterministic(bo *Backoffer, wg *sync.WaitGroup, batch groupedMutations) error {
	mutations := make([]*pb.Mutation, batch.mutations.Len())
	for i := 0; i < batch.mutations.Len(); i++ {
		mutations[i] = &pb.Mutation{
			Op:    batch.mutations.GetOp(i),
			Key:   batch.mutations.GetKey(i),
			Value: batch.mutations.GetValue(i),
		}
	}
	req := tikvrpc.NewRequest(tikvrpc.CmdDeterministicWrite, &pb.DeterministicWriteRequest{
		Mutations:    mutations,
		StartVersion: b.startTS,
		CommitTs:     b.commitTS,
	}, pb.Context{Priority: pb.CommandPri_High, SyncLog: false})

	for {
		sender := NewRegionRequestSender(b.store.regionCache, b.store.client)
		resp, err := sender.SendReq(bo, req, batch.region, readTimeoutShort)

		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = b.writeDeterministicGroups(bo, batch.mutations)
			return errors.Trace(err)
		}
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		prewriteResp := resp.Resp.(*pb.PrewriteResponse)
		keyErrs := prewriteResp.GetErrors()

		if len(keyErrs) == 0 {
			break
		}
	}

	wg.Done()
	return nil
}
