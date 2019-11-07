package session

import (
	"context"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

func getRowsFree(ctx context.Context, sctx sessionctx.Context, rs sqlexec.RecordSet) (error) {
	if rs == nil {
		return nil
	}
	req := rs.NewChunk()
	// Must reuse `req` for imitating server.(*clientConn).writeChunks
	for {
		req.Reset()
		err := rs.Next(ctx, req)
		if err != nil {
			return err
		}
		if req.NumRows() == 0 {
			break
		}

		iter := chunk.NewIterator4Chunk(req)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {}
	}
	return nil
}
