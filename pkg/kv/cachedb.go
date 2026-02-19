// Copyright 2020 PingCAP, Inc.
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

package kv

import (
	"context"
	"encoding/binary"
	"sync"

	"github.com/coocood/freecache"
)

const (
	// globalCacheSize is the size of the global freecache (1GB).
	globalCacheSize = 1024 * 1024 * 1024
	// tsLen is the byte length of an encoded uint64 timestamp.
	tsLen = 8
	// tidLen is the byte length of an encoded int64 table ID.
	tidLen = 8
)

type (
	// CacheDB is the global cache for cached table and point-get reads.
	// It uses a single freecache instance with LRU eviction and supports
	// per-key invalidation via the invalidatedKeys map.
	CacheDB struct {
		cache           *freecache.Cache // 1GB global, stores cached row data
		invalidatedKeys sync.Map         // map[string]uint64 - cacheKey -> min_cached_ts (per-key)
		cachedTables    sync.Map         // map[int64]struct{} - set of registered cached table IDs
	}

	// MemManager adds a cache between transaction buffer and the storage to reduce requests to the storage.
	// Beware, it uses table ID for partition tables, because the keys are unique for partition tables,
	// no matter the physical IDs are the same or not.
	MemManager interface {
		// UnionGet gets the value from cacheDB first, if it not exists,
		// it gets the value from the snapshot, then caches the value in cacheDB.
		UnionGet(ctx context.Context, tid int64, snapshot Snapshot, key Key) ([]byte, error)
		// Delete releases the cache by tableID.
		Delete(tableID int64)
	}
)

// buildCacheKey builds the freecache key: bigEndian(tableID) + originalKVKey.
func buildCacheKey(tid int64, key Key) []byte {
	buf := make([]byte, tidLen+len(key))
	binary.BigEndian.PutUint64(buf, uint64(tid))
	copy(buf[tidLen:], key)
	return buf
}

// encodeCacheValue encodes the cache value: bigEndian(cachedAtTS) + originalKVValue.
func encodeCacheValue(cachedAtTS uint64, value []byte) []byte {
	buf := make([]byte, tsLen+len(value))
	binary.BigEndian.PutUint64(buf, cachedAtTS)
	copy(buf[tsLen:], value)
	return buf
}

// decodeCacheValue decodes the cache value into cachedAtTS and the original value.
func decodeCacheValue(data []byte) (cachedAtTS uint64, value []byte) {
	if len(data) < tsLen {
		return 0, nil
	}
	return binary.BigEndian.Uint64(data[:tsLen]), data[tsLen:]
}

// UnionGet implements MemManager.UnionGet.
// This is the old interface for table-locked reads (EnablePointGetCache).
// It does not perform invalidation checks.
func (c *CacheDB) UnionGet(ctx context.Context, tid int64, snapshot Snapshot, key Key) ([]byte, error) {
	cacheKey := buildCacheKey(tid, key)
	if data, err := c.cache.Get(cacheKey); err == nil {
		_, value := decodeCacheValue(data)
		return value, nil
	}
	val, err := GetValue(ctx, snapshot, key)
	if err != nil {
		return nil, err
	}
	_ = c.cache.Set(cacheKey, encodeCacheValue(0, val), 0)
	return val, nil
}

// CachedUnionGet gets the value for a cached table read with TS-aware invalidation.
// It checks per-key invalidation against readTS, then consults the global
// freecache, and falls back to the snapshot on miss or stale entry.
func (c *CacheDB) CachedUnionGet(ctx context.Context, tid int64, readTS uint64, snapshot Snapshot, key Key) ([]byte, error) {
	cacheKey := buildCacheKey(tid, key)
	cacheKeyStr := string(cacheKey)

	// Step 2: Check invalidatedKeys.
	if v, ok := c.invalidatedKeys.Load(cacheKeyStr); ok {
		minCachedTS := v.(uint64)
		if readTS <= minCachedTS {
			// Bypass cache — fetch from snapshot, do NOT cache the result.
			return GetValue(ctx, snapshot, key)
		}
		// Step 3: CAS delete — only remove if the value hasn't been updated
		// by a concurrent invalidation.
		c.invalidatedKeys.CompareAndDelete(cacheKeyStr, minCachedTS)
	}

	// Step 4: Check freecache.
	if data, err := c.cache.Get(cacheKey); err == nil {
		cachedAtTS, value := decodeCacheValue(data)
		// Re-check invalidatedKeys (may have been updated concurrently).
		if v, ok := c.invalidatedKeys.Load(cacheKeyStr); ok {
			minCachedTS := v.(uint64)
			if cachedAtTS <= minCachedTS {
				// Stale entry, fall through to fetch from snapshot.
				return c.fetchAndCache(ctx, cacheKey, readTS, snapshot, key)
			}
		}
		_ = cachedAtTS
		return value, nil
	}

	// Step 5: Cache miss — fetch from snapshot and cache.
	return c.fetchAndCache(ctx, cacheKey, readTS, snapshot, key)
}

// fetchAndCache fetches a value from the snapshot and caches it.
func (c *CacheDB) fetchAndCache(ctx context.Context, cacheKey []byte, readTS uint64, snapshot Snapshot, key Key) ([]byte, error) {
	val, err := GetValue(ctx, snapshot, key)
	if err != nil {
		return nil, err
	}
	_ = c.cache.Set(cacheKey, encodeCacheValue(readTS, val), 0)
	return val, nil
}

// BatchCachedUnionGet is like CachedUnionGet but for multiple keys.
// Keys that do not exist (ErrNotExist) are silently skipped.
func (c *CacheDB) BatchCachedUnionGet(ctx context.Context, tid int64, readTS uint64, snapshot Snapshot, keys []Key) (map[string][]byte, error) {
	result := make(map[string][]byte, len(keys))
	for _, key := range keys {
		val, err := c.CachedUnionGet(ctx, tid, readTS, snapshot, key)
		if err != nil {
			if ErrNotExist.Equal(err) {
				continue
			}
			return nil, err
		}
		result[string(key)] = val
	}
	return result, nil
}

// InvalidateKeys marks the given keys as invalidated with the specified
// min_cached_ts. For each key, it eagerly evicts the entry from freecache
// and CAS-stores the min_cached_ts in invalidatedKeys (only if the new
// value is greater than the existing one, to handle out-of-order arrivals).
//
// The invalidatedKeys map is self-cleaning: entries are removed via
// CompareAndDelete when a read with readTS > min_cached_ts comes in.
func (c *CacheDB) InvalidateKeys(tid int64, keys []Key, minCachedTS uint64) {
	for _, key := range keys {
		cacheKey := buildCacheKey(tid, key)
		cacheKeyStr := string(cacheKey)
		// Eager eviction of the known-stale entry.
		c.cache.Del(cacheKey)
		// CAS-store min_cached_ts: only store if the new value is greater.
		for {
			existing, loaded := c.invalidatedKeys.LoadOrStore(cacheKeyStr, minCachedTS)
			if !loaded {
				break // Stored successfully (was empty).
			}
			if existing.(uint64) >= minCachedTS {
				break // Existing entry is already newer or equal.
			}
			// Existing is lower, try to upgrade.
			if c.invalidatedKeys.CompareAndSwap(cacheKeyStr, existing, minCachedTS) {
				break
			}
			// CAS failed (concurrent update), retry.
		}
	}
}

// RegisterCachedTable marks a table as a cached table.
func (c *CacheDB) RegisterCachedTable(tableID int64) {
	c.cachedTables.Store(tableID, struct{}{})
}

// UnregisterCachedTable removes a table from the set of cached tables.
func (c *CacheDB) UnregisterCachedTable(tableID int64) {
	c.cachedTables.Delete(tableID)
}

// Delete clears the entire cache. This is used by the old table-lock path
// (EnablePointGetCache) when a table is unlocked. Since the global cache
// does not support efficient per-table deletion, the entire cache is cleared.
// This method is temporary and will be removed when EnablePointGetCache
// is updated to use the new invalidation protocol.
func (c *CacheDB) Delete(_ int64) {
	c.cache.Clear()
}

// NewCacheDB creates a new CacheDB with a 1GB global freecache.
func NewCacheDB() MemManager {
	return &CacheDB{
		cache: freecache.NewCache(globalCacheSize),
	}
}
