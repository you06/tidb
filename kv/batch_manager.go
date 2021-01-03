package kv

import "context"

type BatchManager interface {
	NextBatch(ctx context.Context, txnScope string) (Transaction, error)
}
