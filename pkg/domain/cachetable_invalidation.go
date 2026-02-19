// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"context"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// cachedTableLeaseForPoller is the lease duration for cached table invalidation.
	// The write path waits this duration after writing invalidation entries.
	cachedTableLeaseForPoller = 100 * time.Millisecond
	// pollInterval is the interval between invalidation polls (half the lease).
	pollInterval = cachedTableLeaseForPoller / 2
	// invalidationEntryTTL is how long invalidation entries are kept before cleanup.
	invalidationEntryTTL = 10 * time.Minute
)

// cacheTableInvalidationLoop runs a background goroutine that polls
// mysql.table_cache_invalidation for new invalidation entries and
// applies them to the local CacheDB (freecache).
//
// Each TiDB node runs this loop. It checks every half-lease (50ms)
// for new entries and calls CacheDB.InvalidateKeys to evict stale
// cached data. If polling fails for longer than one full lease,
// the entire cache is cleared as a safety fallback.
func (do *Domain) cacheTableInvalidationLoop() {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	lastPollSuccess := time.Now()
	tableReady := false

	for {
		select {
		case <-do.exit:
			return
		case <-ticker.C:
			if err := do.pollAndApplyInvalidations(); err != nil {
				// Only log at debug level if the table doesn't exist yet (during bootstrap).
				if !tableReady {
					logutil.BgLogger().Debug("cached table invalidation poll skipped (table may not exist yet)",
						zap.Error(err))
				} else {
					logutil.BgLogger().Warn("cached table invalidation poll failed",
						zap.Error(err))
					// Fallback: if polling has failed for longer than one full lease,
					// invalidate all cached data unconditionally.
					if time.Since(lastPollSuccess) >= cachedTableLeaseForPoller {
						cacheDB := do.store.GetMemCache()
						if c, ok := cacheDB.(*kv.CacheDB); ok {
							c.InvalidateAll()
						}
					}
				}
				continue
			}
			tableReady = true
			lastPollSuccess = time.Now()

			// Periodically clean up expired invalidation entries.
			do.cleanupExpiredInvalidations()
		}
	}
}

// pollAndApplyInvalidations reads new invalidation entries from
// mysql.table_cache_invalidation and applies them to the local CacheDB.
func (do *Domain) pollAndApplyInvalidations() error {
	se, err := do.sysSessionPool.Get()
	if err != nil {
		return err
	}
	defer do.sysSessionPool.Put(se)
	sctx := se.(sessionctx.Context)
	exec := sctx.GetSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnCacheTable)

	rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil,
		"SELECT tid, cache_key, min_cached_ts FROM mysql.table_cache_invalidation")
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	cacheDB := do.store.GetMemCache()
	c, ok := cacheDB.(*kv.CacheDB)
	if !ok {
		return nil
	}

	// Group entries by (tid, minCachedTS) for batch invalidation.
	type invalidationGroup struct {
		keys        []kv.Key
		minCachedTS uint64
	}
	groups := make(map[int64]*invalidationGroup)
	for _, row := range rows {
		tid := row.GetInt64(0)
		cacheKey := row.GetBytes(1)
		minCachedTS := row.GetUint64(2)

		g, exists := groups[tid]
		if !exists {
			g = &invalidationGroup{minCachedTS: minCachedTS}
			groups[tid] = g
		}
		g.keys = append(g.keys, kv.Key(cacheKey))
		// Use the maximum minCachedTS for the group.
		if minCachedTS > g.minCachedTS {
			g.minCachedTS = minCachedTS
		}
	}

	for tid, g := range groups {
		c.InvalidateKeys(tid, g.keys, g.minCachedTS)
	}

	_ = exec // keep the exec variable for future use
	return nil
}

// cleanupExpiredInvalidations removes old invalidation entries from
// mysql.table_cache_invalidation that are older than invalidationEntryTTL.
func (do *Domain) cleanupExpiredInvalidations() {
	se, err := do.sysSessionPool.Get()
	if err != nil {
		logutil.BgLogger().Warn("cached table invalidation cleanup: get session failed",
			zap.Error(err))
		return
	}
	defer do.sysSessionPool.Put(se)
	sctx := se.(sessionctx.Context)
	exec := sctx.GetSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnCacheTable)

	// Get the current TSO to compute a cutoff timestamp.
	store := do.store
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		logutil.BgLogger().Warn("cached table invalidation cleanup: get current version failed",
			zap.Error(err))
		return
	}
	// Convert invalidationEntryTTL to a TSO-compatible cutoff.
	// TSO physical part is in milliseconds, so use the current physical time minus TTL.
	cutoffTS := ver.Ver - uint64(invalidationEntryTTL.Milliseconds())<<18

	_, err = exec.ExecuteInternal(ctx,
		"DELETE FROM mysql.table_cache_invalidation WHERE min_cached_ts < %?", cutoffTS)
	if err != nil {
		logutil.BgLogger().Warn("cached table invalidation cleanup: delete failed",
			zap.Error(err))
	}
}
