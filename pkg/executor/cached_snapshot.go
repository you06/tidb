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

package executor

import (
	"context"
	"maps"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
)

// cachedSnapshot wraps a kv.Snapshot and intercepts Get/BatchGet for cached
// tables, routing them through the CacheDB's CachedUnionGet/BatchCachedUnionGet
// with TS-aware invalidation. Iter/IterReverse pass through to the underlying
// snapshot — scans are not cached.
type cachedSnapshot struct {
	kv.Snapshot
	cacheDB      *kv.CacheDB
	cachedTables map[int64]struct{} // set of cached table IDs
	readTS       uint64             // this snapshot's read timestamp
}

// newCachedSnapshot creates a cachedSnapshot that intercepts point reads for the
// given set of cached table IDs, routing them through the CacheDB with TS-aware
// invalidation. Scans (Iter/IterReverse) pass through to the underlying snapshot.
func newCachedSnapshot(snapshot kv.Snapshot, cacheDB *kv.CacheDB, readTS uint64, cachedTableIDs []int64) *cachedSnapshot {
	tables := make(map[int64]struct{}, len(cachedTableIDs))
	for _, id := range cachedTableIDs {
		tables[id] = struct{}{}
	}
	return &cachedSnapshot{
		Snapshot:     snapshot,
		cacheDB:      cacheDB,
		cachedTables: tables,
		readTS:       readTS,
	}
}

// Get intercepts point reads for cached tables, routing them through the CacheDB.
// Keys belonging to non-cached tables are forwarded to the underlying snapshot.
func (s *cachedSnapshot) Get(ctx context.Context, key kv.Key, options ...kv.GetOption) (kv.ValueEntry, error) {
	tableID := tablecodec.DecodeTableID(key)
	if _, ok := s.cachedTables[tableID]; ok {
		val, err := s.cacheDB.CachedUnionGet(ctx, tableID, s.readTS, s.Snapshot, key)
		if err != nil {
			return kv.ValueEntry{}, err
		}
		return kv.NewValueEntry(val, 0), nil
	}
	return s.Snapshot.Get(ctx, key, options...)
}

// BatchGet intercepts batch point reads. Keys belonging to cached tables are
// grouped by table ID and routed through the CacheDB's BatchCachedUnionGet.
// Uncached keys are forwarded to the underlying snapshot's BatchGet. Results
// are merged into a single map.
func (s *cachedSnapshot) BatchGet(ctx context.Context, keys []kv.Key, options ...kv.BatchGetOption) (map[string]kv.ValueEntry, error) {
	cachedKeysByTable := make(map[int64][]kv.Key)
	var uncachedKeys []kv.Key

	for _, key := range keys {
		tableID := tablecodec.DecodeTableID(key)
		if _, ok := s.cachedTables[tableID]; ok {
			cachedKeysByTable[tableID] = append(cachedKeysByTable[tableID], key)
		} else {
			uncachedKeys = append(uncachedKeys, key)
		}
	}

	result := make(map[string]kv.ValueEntry, len(keys))

	// Process cached keys through CacheDB, grouped by table ID.
	for tableID, tableKeys := range cachedKeysByTable {
		vals, err := s.cacheDB.BatchCachedUnionGet(ctx, tableID, s.readTS, s.Snapshot, tableKeys)
		if err != nil {
			return nil, err
		}
		for k, v := range vals {
			result[k] = kv.NewValueEntry(v, 0)
		}
	}

	// Process uncached keys through the underlying snapshot.
	if len(uncachedKeys) > 0 {
		snapResult, err := s.Snapshot.BatchGet(ctx, uncachedKeys, options...)
		if err != nil {
			return nil, err
		}
		maps.Copy(result, snapResult)
	}

	return result, nil
}
