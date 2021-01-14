package tikv

import (
	"context"
	"encoding/hex"
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
	batchStateFree uint32 = 1 << iota
	batchStateStarting
	batchStateExecuting
	batchStateDetecting
	batchStateCommitting

	batchStateCanNext = batchStateFree | batchStateStarting

	txnStateInit uint64 = iota
	txnStateBeforeCommit
	CheckPointKey = "checkpoint"
)

type batchFuture struct {
	bm *batchManager
}

func (b *batchFuture) Wait() (uint64, error) {
	bm := b.bm
	bm.startMutex.Lock()
	for atomic.LoadUint32(&bm.state) == batchStateStarting {
		bm.startReady.Wait()
	}
	bm.startMutex.Unlock()
	return bm.startTS, bm.startErr
}

type batchManager struct {
	state       uint32
	futureCount uint32
	txnCount    uint32
	readyCount  uint32
	startTS     uint64
	commitTS    uint64
	keyLen      uint64

	mu            sync.Mutex
	conflictMu    sync.Mutex
	freeMutex     sync.Mutex
	freeReady     *sync.Cond
	startMutex    sync.Mutex
	startReady    *sync.Cond
	detectMutex   sync.Mutex
	detectCond    *sync.Cond
	commitMutex   sync.Mutex
	commitReady   *sync.Cond
	clearReady    sync.WaitGroup
	mutations     *PlainMutations
	lockMutations *PlainMutations
	startErr      error
	errMutex      sync.Mutex
	commitErrs    map[uint64]error
	store         *tikvStore
	vars          *kv.Variables
	prevTxns      map[uint32]*tikvTxn
	txns          map[uint32]*tikvTxn
	conflictTxns  map[uint32]struct{}
	//thisBatch     map[uint64]struct{}
}

func newBatchManager(store *tikvStore) (*batchManager, error) {
	bm := batchManager{
		state:        batchStateFree,
		store:        store,
		txns:         make(map[uint32]*tikvTxn),
		vars:         kv.DefaultVars,
		commitErrs:   make(map[uint64]error),
		conflictTxns: make(map[uint32]struct{}),
		//thisBatch:    make(map[uint64]struct{}),
	}
	bm.freeReady = sync.NewCond(&bm.freeMutex)
	bm.startReady = sync.NewCond(&bm.startMutex)
	bm.detectCond = sync.NewCond(&bm.detectMutex)
	bm.commitReady = sync.NewCond(&bm.commitMutex)
	return &bm, nil
}

// NextBatch return next batch's startTS when it's ready
func (b *batchManager) NextBatch(ctx context.Context, connID uint64) oracle.Future {
	// logutil.Logger(ctx).Info("MYLOG call NextBatch", zap.Stack("trace"))
WAIT:
	b.freeMutex.Lock()
	for atomic.LoadUint32(&b.state)&batchStateCanNext == 0 {
		b.freeReady.Wait()
	}
	if b.txnCount == 0 {
		b.futureCount++
		futureVal := atomic.LoadUint32(&b.futureCount)
		logutil.Logger(ctx).Info("MYLOG add future", zap.Uint32("future", futureVal))
	} else {
		futureVal := atomic.LoadUint32(&b.futureCount)
		logutil.Logger(ctx).Info("MYLOG wait for next", zap.Uint32("future", futureVal))
		b.freeMutex.Unlock()
		goto WAIT
	}
	b.freeMutex.Unlock()

	//atomic.AddUint32(&b.futureCount, 1)
	if atomic.CompareAndSwapUint32(&b.state, batchStateFree, batchStateStarting) {
		// allocate a checkpoint for first txn in a batch
		go b.writeCheckpointStart()
	}

	return &batchFuture{bm: b}
}

func (b *batchManager) Clear() {
	//logutil.BgLogger().Info("MYLOG wait clear ready", zap.Uint64("start ts", b.startTS))
	b.clearReady.Wait()
	logutil.BgLogger().Info("MYLOG wait clear ready done", zap.Uint64("start ts", b.startTS))
	b.mu.Lock()
	defer b.mu.Unlock()
	b.futureCount = 0
	b.txnCount = 0
	b.readyCount = 0
	b.startTS = 0
	b.commitTS = 0
	b.mutations = nil
	b.keyLen = 0
	b.startErr = nil
	//b.commitErr = nil
	b.prevTxns = b.txns
	b.txns = make(map[uint32]*tikvTxn)
	b.conflictTxns = make(map[uint32]struct{})
	//b.thisBatch = make(map[uint64]struct{})
	atomic.StoreUint32(&b.state, batchStateFree)
}

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
	delete(b.txns, txn.snapshot.replicaReadSeed)
	b.txnCount--
	logutil.BgLogger().Info("MYLOG call remove ready",
		zap.Uint64("startTS", txn.startTS),
		zap.Uint32("ready count", b.readyCount),
		zap.Uint32("txn count", b.txnCount))
	if b.readyCount == b.txnCount {
		//logutil.BgLogger().Info("MYLOG trigger detectConflicts remove",
		//	zap.Uint64("startTS", b.startTS),
		//	zap.Uint32("txnCount", b.txnCount))
		b.clearReady.Add(int(b.txnCount))
		go b.detectConflicts()
	}
}

func (b *batchManager) mutationReady(txn *tikvTxn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.txns[txn.snapshot.replicaReadSeed] = txn
	b.readyCount++
	logutil.BgLogger().Info("MYLOG call mutation ready",
		zap.Uint64("startTS", txn.startTS),
		zap.Uint32("ready count", b.readyCount),
		zap.Uint32("txn count", b.txnCount))
	if b.readyCount == b.txnCount {
		//logutil.BgLogger().Info("MYLOG add clear ready",
		//	zap.Uint64("startTS", b.startTS),
		//	zap.Int("cnt", int(b.txnCount)))
		b.clearReady.Add(int(b.txnCount))
		go b.detectConflicts()
	}
}

// txnSignature is used for checking whether a key is in this txn's mutation
type txnSignature struct {
	sync.RWMutex
	conflict bool
	// TODO: use a bloom filter for large txn
	bytes  []byte
	keyMap map[string]struct{}
}

const keyHintSize = 20

func extractSignature(mutation *memBufferMutations) txnSignature {
	bytes := make([]byte, 0, keyHintSize)
	keyMap := make(map[string]struct{})
	keys := mutation.GetKeys()
	for _, key := range keys {
		keyMap[hex.EncodeToString(key)] = struct{}{}
		for i, b := range key {
			if i == len(bytes) {
				bytes = append(bytes, b)
			} else {
				bytes[i] |= b
			}
		}
	}
	return txnSignature{
		conflict: false,
		bytes:    bytes,
		keyMap:   keyMap,
	}
}

func (b *batchManager) detectConflicts() {
	logutil.BgLogger().Info("MYLOG detect conflict")

	atomic.StoreUint32(&b.state, batchStateDetecting)

	// detection
	var (
		txnSignatures = make([]txnSignature, b.txnCount)
		txns          = make([]*tikvTxn, b.txnCount)
		mutations     = make([]*memBufferMutations, b.txnCount)
		//conflictTxns  = make([]*tikvTxn, 0, b.txnCount)
		wg sync.WaitGroup
	)
	wg.Add(int(b.txnCount))
	tID := 0
	for _, txn := range b.txns {
		go func(tID int, txn *tikvTxn) {
			mutation := txn.committer.GetMutations()
			txns[tID] = txn
			mutations[tID] = mutation
			txnSignatures[tID] = extractSignature(mutation)
			wg.Done()
		}(tID, txn)
		tID++
	}
	//logutil.BgLogger().Info("MYLOG detect conflict", zap.Int("txns", tID), zap.Uint32("txnCount", b.txnCount))
	wg.Wait()

	wg.Add(int(b.txnCount))
	for i := tID - 1; i >= 0; i-- {
		go func(i int) {
			var (
				signature txnSignature
				keyStr    string
				ok        bool
				conflict  = false
			)
			mutation := mutations[i]
			keys := mutation.GetKeys()
		TXN:
			for j := 0; j < i; j++ {
				signature = txnSignatures[j]
				signature.RLock()
				ok = signature.conflict
				signature.RUnlock()
				if ok {
					continue
				}
			KEY:
				for _, key := range keys {
					//logutil.BgLogger().Info("MYLOG loop keys",
					//	zap.Int("txn", i),
					//	zap.ByteString("signature", signature.bytes),
					//	zap.ByteString("key", key))
					if len(key) > len(signature.bytes) {
						continue
					}
					for k := 0; k < len(key); k++ {
						if (signature.bytes[k] & key[k]) != key[k] {
							continue KEY
						}
					}
					keyStr = hex.EncodeToString(key)
					signature.Lock()
					_, ok = signature.keyMap[keyStr]
					//logutil.BgLogger().Info("MYLOG loop keys p1",
					//	zap.Int("txn", i),
					//	zap.ByteString("key", key),
					//	zap.Bool("ok", ok))
					signature.Unlock()
					if ok {
						signature = txnSignatures[i]
						signature.Lock()
						signature.conflict = true
						signature.Unlock()
						conflict = true
						break TXN
					}
				}
			}

			if conflict {
				b.mu.Lock()
				delete(b.txns, txns[i].snapshot.replicaReadSeed)
				b.mu.Unlock()
				b.conflictMu.Lock()
				b.conflictTxns[txns[i].snapshot.replicaReadSeed] = struct{}{}
				b.conflictMu.Unlock()
			} else {
				atomic.AddUint64(&b.keyLen, uint64(mutation.Len()))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	logutil.BgLogger().Info("MYLOG detect conflict done")

	b.detectMutex.Lock()
	atomic.StoreUint32(&b.state, batchStateCommitting)
	b.detectCond.Broadcast()
	b.detectMutex.Unlock()

	//b.commitDone.Add(len(b.txns) - 1)
	var (
		commitTS = b.commitTS
		err      error
	)
	err = b.commit()
	if err != nil {
		//logutil.BgLogger().Info("MYLOG check commit err", zap.Error(err))
		b.errMutex.Lock()
		b.commitErrs[commitTS] = err
		b.errMutex.Unlock()
	} else {
		//logutil.BgLogger().Info("MYLOG check commit no err")
	}
	b.Clear()
	b.freeReady.Broadcast()
}

func (b *batchManager) hasConflict(txn *tikvTxn) bool {
	b.detectMutex.Lock()
	for atomic.LoadUint32(&b.state) < batchStateCommitting {
		b.detectCond.Wait()
	}
	b.detectMutex.Unlock()
	b.conflictMu.Lock()
	_, ok := b.conflictTxns[txn.snapshot.replicaReadSeed]
	b.conflictMu.Unlock()
	logutil.BgLogger().Info("MYLOG call hasConflict",
		zap.Uint64("startTS", txn.startTS),
		zap.Uint64("batch startTS", b.startTS),
		zap.Bool("eq", txn.startTS == b.startTS),
		zap.Bool("conflict", ok))

	b.clearReady.Done()
	return ok
}

func (b *batchManager) getCommitErr(commitTS uint64) error {
	//logutil.BgLogger().Info("MYLOG try get commit err", zap.Uint64("commitTS", commitTS))
	batchCommitTS := atomic.LoadUint64(&b.commitTS)
	if batchCommitTS == commitTS {
		b.freeMutex.Lock()
		for atomic.LoadUint32(&b.state) != batchStateFree {
			b.freeReady.Wait()
		}
		b.freeMutex.Unlock()
	}
	//logutil.BgLogger().Info("MYLOG got commit err", zap.Uint64("commitTS", commitTS))
	b.errMutex.Lock()
	defer b.errMutex.Unlock()
	return b.commitErrs[commitTS]
}

func (b *batchManager) newCheckpointBackOffer() *Backoffer {
	bo := NewBackofferWithVars(context.Background(), tsoMaxBackoff, nil)
	return bo
}

// TODO: handle PD server timeout
func (b *batchManager) writeCheckpointStart() {
	// write checkpointStart

	bo := b.newCheckpointBackOffer()

	time.Sleep(1000 * time.Microsecond)
	//time.Sleep(time.Second)
	b.startTS, b.startErr = b.store.getTimestampWithRetry(bo, oracle.GlobalTxnScope)
	b.commitTS = b.startTS + 1

	b.freeMutex.Lock()
	atomic.StoreUint32(&b.state, batchStateExecuting)
	b.txnCount = b.futureCount
	b.freeMutex.Unlock()

	b.startMutex.Lock()
	b.startReady.Broadcast()
	b.startMutex.Unlock()

	logutil.BgLogger().Info("MYLOG got startTS",
		zap.Uint64("startTS", b.startTS),
		zap.Uint32("txn count", b.txnCount))
}

func (b *batchManager) writeCheckpointCommit() error {
	return nil
}

func (b *batchManager) writeCheckpointRollback() error {
	return nil
}

func (b *batchManager) mergeMutations() {
	var (
		ops                = make([]pb.Op, int(b.keyLen))
		keys               = make([][]byte, int(b.keyLen))
		values             = make([][]byte, int(b.keyLen))
		isPessimisticLocks = make([]bool, int(b.keyLen))
		start              = 0
		wg                 sync.WaitGroup
	)

	wg.Add(len(b.txns))
	for _, txn := range b.txns {
		mutation := txn.committer.GetMutations()
		go func(mutation *memBufferMutations, start int) {
			for i := 0; i < mutation.Len(); i++ {
				ops[start+i] = mutation.GetOp(i)
				keys[start+i] = mutation.GetKey(i)
				values[start+i] = mutation.GetValue(i)
			}
			wg.Done()
		}(mutation, start)
		start += mutation.Len()
	}
	wg.Wait()
	b.mutations = &PlainMutations{
		ops:               ops,
		keys:              keys,
		values:            values,
		isPessimisticLock: isPessimisticLocks,
	}
	b.lockMutations = &PlainMutations{keys: keys}
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
			//logutil.BgLogger().Info("deterministic detect large amount of mutations on a single region",
			//	zap.Uint64("region", group.region.GetID()),
			//	zap.Int("mutations count", group.mutations.Len()))
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
	b.mergeMutations()
	logutil.BgLogger().Info("MYLOG write locks", zap.Int("txn count", len(b.txns)))
	bo := NewBackofferWithVars(context.Background(), pessimisticLockMaxBackoff, b.vars)
	err := b.lockDeterministicGroups(bo, b.lockMutations)
	if err != nil {
		return err
	}
	logutil.BgLogger().Info("MYLOG start commit")
	bo = NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, b.vars)
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
		logutil.BgLogger().Info("MYLOG group mutations fail", zap.Error(err))
		return err
	}

	groupWg.Add(len(groups))
	for _, group := range groups {
		go func(group groupedMutations) {
			writeBo := NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, b.vars)
			err := b.writeDeterministic(writeBo, &groupWg, group)
			if err != nil {
				logutil.BgLogger().Info("MYLOG group write fail", zap.Error(err))
			}
		}(group)
	}
	groupWg.Wait()

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
		//logutil.BgLogger().Info("MYLOG, send commit write req", zap.String("mutations", fmt.Sprintln(mutations)))
		//logutil.BgLogger().Info("MYLOG, send commit write req")
		sender := NewRegionRequestSender(b.store.regionCache, b.store.client)
		resp, err := sender.SendReq(bo, req, batch.region, readTimeoutShort)
		//logutil.BgLogger().Info("MYLOG, got commit write res", zap.Error(err))

		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			//logutil.BgLogger().Info("MYLOG, get region error failed res", zap.Error(err))
			return errors.Trace(err)
		}
		if regionErr != nil {
			logutil.BgLogger().Info("MYLOG, commit get region error and restart", zap.String("region err", regionErr.String()))
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = b.writeDeterministicGroups(bo, batch.mutations)
			if err == nil {
				break
			} else {
				return errors.Trace(err)
			}
		}
		if resp.Resp == nil {
			//logutil.BgLogger().Info("MYLOG, body missing")
			return errors.Trace(ErrBodyMissing)
		}

		if writeResponse, ok := resp.Resp.(*pb.DeterministicWriteResponse); ok {
			errs := writeResponse.GetErrors()
			if len(errs) == 0 {
				break
				//logutil.BgLogger().Info("MYLOG, resp got no err")
			} else {
				for _, err := range errs {
					logutil.BgLogger().Info("MYLOG, resp err", zap.Stringer("err", err))
				}
			}
		}
		break
	}

	wg.Done()
	return nil
}

func (b *batchManager) lockDeterministicGroups(bo *Backoffer, mutations CommitterMutations) error {
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
			writeBo := NewBackofferWithVars(context.Background(), pessimisticLockMaxBackoff, b.vars)
			b.lockDeterministic(writeBo, &groupWg, group)
		}(group)
	}
	groupWg.Wait()

	return nil
}

func (b *batchManager) lockDeterministic(bo *Backoffer, wg *sync.WaitGroup, batch groupedMutations) error {
	mutations := make([]*pb.Mutation, batch.mutations.Len())
	for i := 0; i < batch.mutations.Len(); i++ {
		mutations[i] = &pb.Mutation{
			Op:  pb.Op_PessimisticLock,
			Key: batch.mutations.GetKey(i),
		}
	}
	//logutil.BgLogger().Info("MYLOG, write req mutation", zap.String("mutation", fmt.Sprintln(mutations)))
	req := tikvrpc.NewRequest(tikvrpc.CmdPessimisticLock, &pb.PessimisticLockRequest{
		Mutations:    mutations,
		PrimaryLock:  mutations[0].GetKey(),
		StartVersion: b.startTS,
		ForUpdateTs:  b.commitTS,
		LockTtl:      atomic.LoadUint64(&ManagedLockTTL),
		WaitTimeout:  0,
		ReturnValues: false,
		MinCommitTs:  1,
	}, pb.Context{Priority: pb.CommandPri_High, SyncLog: false})

	for {
		//logutil.BgLogger().Info("MYLOG, send lock write req", zap.String("mutations", fmt.Sprintln(mutations)))
		sender := NewRegionRequestSender(b.store.regionCache, b.store.client)
		resp, err := sender.SendReq(bo, req, batch.region, readTimeoutShort)
		//logutil.BgLogger().Info("MYLOG, got lock write res", zap.Error(err))

		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			//logutil.BgLogger().Info("MYLOG, lock get region error failed res", zap.Error(err))
			return errors.Trace(err)
		}
		if regionErr != nil {
			logutil.BgLogger().Info("MYLOG, lock get region error and restart", zap.String("region err", regionErr.String()))
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = b.lockDeterministicGroups(bo, batch.mutations)
			if err == nil {
				break
			} else {
				return errors.Trace(err)
			}
		}
		if resp.Resp == nil {
			//logutil.BgLogger().Info("MYLOG, body missing")
			return errors.Trace(ErrBodyMissing)
		}

		if writeResponse, ok := resp.Resp.(*pb.PessimisticLockResponse); ok {
			errs := writeResponse.GetErrors()
			if len(errs) == 0 {
				break
				//logutil.BgLogger().Info("MYLOG, resp got no err")
			} else {
				for _, err := range errs {
					logutil.BgLogger().Info("MYLOG, resp err", zap.Stringer("err", err))
				}
			}
		}
		break
	}

	wg.Done()
	return nil
}
