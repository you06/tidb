package kv

import "context"

type BatchManager interface {
	NextBatch(ctx context.Context) (uint64, error)
}
