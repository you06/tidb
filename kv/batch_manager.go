package kv

import (
	"context"

	"github.com/pingcap/tidb/store/tikv/oracle"
)

type BatchManager interface {
	NextBatch(ctx context.Context) oracle.Future
}
