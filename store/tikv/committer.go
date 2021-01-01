package tikv

import (
	"context"
	"sync"

	"github.com/pingcap/tidb/kv"

	"github.com/pingcap/tidb/util/execdetails"
)

type Committer interface {
	GetStartTS() uint64
	GetCommitTS() uint64
	GetMutations() *memBufferMutations
	GetTtlManager() *ttlManager
	GetCleanWg() *sync.WaitGroup
	initKeysAndMutations() error

	isAsyncCommit() bool
	getDetail() *execdetails.CommitDetails
	execute(context.Context) error
	pessimisticRollbackMutations(bo *Backoffer, mutations CommitterMutations) error
	extractKeyExistsErr(key kv.Key) error
	cleanup(ctx context.Context)
}
