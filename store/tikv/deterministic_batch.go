package tikv

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	CheckPointKey = "checkpoint"

	batchStateFree uint32 = iota
	batchStateStarting
	batchStateProgress

	txnStateInit uint64 = iota
	txnStateBeforeCommit
)

type batchFuture struct {
	bm *batchManager
}

func (b *batchFuture) Wait() (uint64, error) {
	bm := b.bm
	bm.startReady.Wait()
	return bm.startTS, bm.startErr
}

type batchManager struct {
	state       uint32
	futureCount uint32
	txnCount    uint32
	startTS     uint64
	commitTS    uint64

	mu         sync.RWMutex
	freeReady  sync.WaitGroup
	startReady sync.WaitGroup
	commitDone sync.WaitGroup
	detectDone sync.WaitGroup
	mutations  *PlainMutations
	startErr   error
	commitErr  error
	store      *tikvStore
	vars       *kv.Variables
	prevTxns   map[uint32]*tikvTxn
	txns       map[uint32]*tikvTxn
	//conflictTxns map[uint32]*tikvTxn
}

func newBatchManager(store *tikvStore) (*batchManager, error) {
	return &batchManager{
		state: batchStateFree,
		store: store,
		txns:  make(map[uint32]*tikvTxn),
		vars:  kv.DefaultVars,
	}, nil
}

// NextBatch return next batch's startTS when it's ready
func (b *batchManager) NextBatch(ctx context.Context) oracle.Future {
	//logutil.Logger(ctx).Info("MYLOG call NextBatch", zap.Stack("trace"))
	b.commitDone.Wait()
	b.freeReady.Wait()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.futureCount++
	if b.state == batchStateFree {
		b.state = batchStateStarting
		b.startReady.Add(1)
		// allocate a checkpoint for first txn in a batch
		go b.writeCheckpointStart()
	}

	return &batchFuture{bm: b}
}

func (b *batchManager) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = batchStateFree
	b.futureCount = 0
	b.txnCount = 0
	b.startTS = 0
	b.commitTS = 0
	b.mutations = nil
	b.startErr = nil
	b.commitErr = nil
	b.prevTxns = b.txns
	b.txns = make(map[uint32]*tikvTxn)
}

//func (b *batchManager) begin() {
//	b.mu.Lock()
//	defer b.mu.Unlock()
//	b.startReady.Add(1)
//}

func (b *batchManager) removeTxn(txn *tikvTxn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.txns[txn.snapshot.replicaReadSeed]; !ok {
		panic("unreachable")
	}
	delete(b.txns, txn.snapshot.replicaReadSeed)
	//if int(b.txnCount) == len(b.txns) {
	//	logutil.BgLogger().Info("MYLOG trigger detectConflicts", zap.Uint32("txn", txn.snapshot.replicaReadSeed))
	//	go b.detectConflicts()
	//}
}

func (b *batchManager) removeTxnReady(txn *tikvTxn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	//if _, ok := b.txns[txn.snapshot.replicaReadSeed]; !ok {
	//	panic("unreachable")
	//}
	//delete(b.txns, txn.snapshot.replicaReadSeed)
	b.futureCount--
	if b.txnCount == b.futureCount {
		//logutil.BgLogger().Info("MYLOG trigger detectConflicts remove",
		//	zap.Uint32("txn", txn.snapshot.replicaReadSeed),
		//	zap.Uint64("startTS", b.startTS),
		//	zap.Uint32("txnCount", b.txnCount),
		//	zap.Uint32("futureCount", b.futureCount))
		go b.detectConflicts()
	}
}

func (b *batchManager) mutationReady(txn *tikvTxn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.txns[txn.snapshot.replicaReadSeed] = txn
	b.txnCount++
	if b.txnCount == b.futureCount {
		//logutil.BgLogger().Info("MYLOG trigger detectConflicts ready",
		//	zap.Uint32("txn", txn.snapshot.replicaReadSeed),
		//	zap.Uint64("startTS", b.startTS),
		//	zap.Uint32("txnCount", b.txnCount),
		//	zap.Uint32("futureCount", b.futureCount))
		go b.detectConflicts()
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
	//logutil.BgLogger().Info("MYLOG, get commit err")
	b.freeReady.Wait()
	defer b.commitDone.Done()
	//logutil.BgLogger().Info("MYLOG, get commit err ok")
	return b.commitErr
}

func (b *batchManager) newCheckpointBackOffer() *Backoffer {
	bo := NewBackofferWithVars(context.Background(), tsoMaxBackoff, nil)
	return bo
}

// TODO: handle PD server timeout
func (b *batchManager) writeCheckpointStart() {
	// write checkpointStart

	bo := b.newCheckpointBackOffer()

	time.Sleep(1500 * time.Microsecond)
	b.startTS, b.startErr = b.store.getTimestampWithRetry(bo, oracle.GlobalTxnScope)
	b.commitTS = b.startTS + 1

	logutil.BgLogger().Info("MYLOG got startTS", zap.Uint64("startTS", b.startTS))
	b.freeReady.Add(1)
	b.detectDone.Add(1)
	b.commitDone.Add(1)
	b.startReady.Done()
}

func (b *batchManager) writeCheckpointCommit() error {
	b.Clear()
	b.freeReady.Done()
	return nil
}

func (b *batchManager) writeCheckpointRollback() error {

	b.freeReady.Done()
	return nil
}

func (b *batchManager) mergeMutations() {
	var (
		mutation  *memBufferMutations
		mutations PlainMutations
		sizeHint  = 0
	)
	for _, txn := range b.txns {
		mutation = txn.committer.GetMutations()
		sizeHint += mutation.Len()
	}
	mutations = NewPlainMutations(sizeHint)
	for _, txn := range b.txns {
		mutation = txn.committer.GetMutations()
		var (
			op    pb.Op
			key   []byte
			value []byte
		)
		for i := 0; i < mutation.Len(); i++ {
			op = mutation.GetOp(i)
			// ignore pessimistic
			if op == pb.Op_Lock {
				continue
			}
			key = mutation.GetKey(i)
			value = mutation.GetValue(i)

			mutations.Push(op, key, value, false)
		}
	}
	b.mutations = &mutations
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
			logutil.BgLogger().Info("MYLOG, write batch")
			writeBo := NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, b.vars)
			b.writeDeterministic(writeBo, &groupWg, group)
		}(group)
	}
	logutil.BgLogger().Info("MYLOG, waiting write batch")
	groupWg.Wait()
	logutil.BgLogger().Info("MYLOG, done write batch")

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
	//logutil.BgLogger().Info("MYLOG, write req mutation", zap.String("mutation", fmt.Sprintln(mutations)))
	req := tikvrpc.NewRequest(tikvrpc.CmdDeterministicWrite, &pb.DeterministicWriteRequest{
		Mutations:    mutations,
		StartVersion: b.startTS,
		CommitTs:     b.commitTS,
	}, pb.Context{Priority: pb.CommandPri_High, SyncLog: false})

	for {
		logutil.BgLogger().Info("MYLOG, send write req")
		sender := NewRegionRequestSender(b.store.regionCache, b.store.client)
		resp, err := sender.SendReq(bo, req, batch.region, readTimeoutShort)
		logutil.BgLogger().Info("MYLOG, got write res", zap.Error(err))

		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			logutil.BgLogger().Info("MYLOG, get region error failed res", zap.Error(err))
			return errors.Trace(err)
		}
		if regionErr != nil {
			logutil.BgLogger().Info("MYLOG, get region error and restart", zap.String("region err", regionErr.String()))
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = b.writeDeterministicGroups(bo, batch.mutations)
			return errors.Trace(err)
		}
		if resp.Resp == nil {
			logutil.BgLogger().Info("MYLOG, body missing")
			return errors.Trace(ErrBodyMissing)
		}

		if writeResponse, ok := resp.Resp.(*pb.DeterministicWriteResponse); ok {
			errs := writeResponse.GetErrors()
			if len(errs) == 0 {
				logutil.BgLogger().Info("MYLOG, resp got no err")
			}
			for _, err := range errs {
				logutil.BgLogger().Info("MYLOG, resp err", zap.Stringer("err", err))
			}
		}
		break
	}

	wg.Done()
	return nil
}
