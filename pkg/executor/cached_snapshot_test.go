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
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSnapshot is a simple in-memory snapshot for testing cachedSnapshot.
type mockSnapshot struct {
	data   map[string][]byte
	getCnt int
	mu     sync.Mutex
}

func newMockSnapshot(data map[string][]byte) *mockSnapshot {
	return &mockSnapshot{data: data}
}

func (s *mockSnapshot) Get(_ context.Context, k kv.Key, _ ...kv.GetOption) (kv.ValueEntry, error) {
	s.mu.Lock()
	s.getCnt++
	s.mu.Unlock()
	if v, ok := s.data[string(k)]; ok {
		return kv.NewValueEntry(v, 0), nil
	}
	return kv.ValueEntry{}, kv.ErrNotExist
}

func (s *mockSnapshot) BatchGet(ctx context.Context, keys []kv.Key, _ ...kv.BatchGetOption) (map[string]kv.ValueEntry, error) {
	m := make(map[string]kv.ValueEntry, len(keys))
	for _, k := range keys {
		v, err := s.Get(ctx, k)
		if kv.IsErrNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		m[string(k)] = v
	}
	return m, nil
}

func (s *mockSnapshot) Iter(kv.Key, kv.Key) (kv.Iterator, error)        { return nil, nil }
func (s *mockSnapshot) IterReverse(kv.Key, kv.Key) (kv.Iterator, error) { return nil, nil }
func (s *mockSnapshot) SetOption(int, any)                               {}

func (s *mockSnapshot) getCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getCnt
}

// makeRowKey creates a table record key for the given tableID and handle.
func makeRowKey(tableID int64, handle int64) kv.Key {
	return tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(handle))
}

func newTestCacheDBForSnapshot() *kv.CacheDB {
	return kv.NewCacheDB().(*kv.CacheDB)
}

func TestCachedSnapshotGetCachedTable(t *testing.T) {
	cdb := newTestCacheDBForSnapshot()
	key := makeRowKey(1, 100)
	snap := newMockSnapshot(map[string][]byte{
		string(key): []byte("value1"),
	})

	cs := newCachedSnapshot(snap, cdb, 200, []int64{1})
	ctx := context.Background()

	// First Get: cache miss, fetches from snapshot via CacheDB.
	entry, err := cs.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), entry.Value)
	assert.Equal(t, 1, snap.getCount())

	// Second Get: cache hit, no snapshot fetch.
	entry, err = cs.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), entry.Value)
	assert.Equal(t, 1, snap.getCount())
}

func TestCachedSnapshotGetUncachedTable(t *testing.T) {
	cdb := newTestCacheDBForSnapshot()
	key := makeRowKey(2, 100) // table 2 is NOT cached
	snap := newMockSnapshot(map[string][]byte{
		string(key): []byte("value2"),
	})

	cs := newCachedSnapshot(snap, cdb, 200, []int64{1}) // only table 1 is cached
	ctx := context.Background()

	// Get for uncached table goes directly to snapshot.
	entry, err := cs.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("value2"), entry.Value)
	assert.Equal(t, 1, snap.getCount())

	// Second Get also goes to snapshot (no caching).
	entry, err = cs.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("value2"), entry.Value)
	assert.Equal(t, 2, snap.getCount())
}

func TestCachedSnapshotGetNotExist(t *testing.T) {
	cdb := newTestCacheDBForSnapshot()
	key := makeRowKey(1, 999)
	snap := newMockSnapshot(map[string][]byte{})

	cs := newCachedSnapshot(snap, cdb, 200, []int64{1})
	ctx := context.Background()

	_, err := cs.Get(ctx, key)
	require.Error(t, err)
	assert.True(t, kv.ErrNotExist.Equal(err))
}

func TestCachedSnapshotGetWithInvalidation(t *testing.T) {
	cdb := newTestCacheDBForSnapshot()
	key := makeRowKey(1, 100)
	snap := newMockSnapshot(map[string][]byte{
		string(key): []byte("value1"),
	})

	cs := newCachedSnapshot(snap, cdb, 150, []int64{1})
	ctx := context.Background()

	// Populate cache.
	entry, err := cs.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), entry.Value)
	assert.Equal(t, 1, snap.getCount())

	// Invalidate with min_cached_ts=200.
	cdb.InvalidateKeys(1, []kv.Key{key}, 200)

	// Read with readTS=150 <= min_cached_ts=200: bypasses cache.
	entry, err = cs.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), entry.Value)
	assert.Equal(t, 2, snap.getCount())
}

func TestCachedSnapshotBatchGetMixed(t *testing.T) {
	cdb := newTestCacheDBForSnapshot()
	cachedKey1 := makeRowKey(1, 100) // cached table 1
	cachedKey2 := makeRowKey(1, 200) // cached table 1
	uncachedKey := makeRowKey(2, 100) // uncached table 2

	snap := newMockSnapshot(map[string][]byte{
		string(cachedKey1):  []byte("cached1"),
		string(cachedKey2):  []byte("cached2"),
		string(uncachedKey): []byte("uncached"),
	})

	cs := newCachedSnapshot(snap, cdb, 200, []int64{1})
	ctx := context.Background()

	keys := []kv.Key{cachedKey1, cachedKey2, uncachedKey}
	result, err := cs.BatchGet(ctx, keys)
	require.NoError(t, err)
	assert.Len(t, result, 3)
	assert.Equal(t, []byte("cached1"), result[string(cachedKey1)].Value)
	assert.Equal(t, []byte("cached2"), result[string(cachedKey2)].Value)
	assert.Equal(t, []byte("uncached"), result[string(uncachedKey)].Value)
}

func TestCachedSnapshotBatchGetAllCached(t *testing.T) {
	cdb := newTestCacheDBForSnapshot()
	key1 := makeRowKey(1, 100)
	key2 := makeRowKey(1, 200)

	snap := newMockSnapshot(map[string][]byte{
		string(key1): []byte("v1"),
		string(key2): []byte("v2"),
	})

	cs := newCachedSnapshot(snap, cdb, 200, []int64{1})
	ctx := context.Background()

	// First batch: all cache misses, fetched from snapshot.
	result, err := cs.BatchGet(ctx, []kv.Key{key1, key2})
	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, []byte("v1"), result[string(key1)].Value)
	assert.Equal(t, []byte("v2"), result[string(key2)].Value)
	assert.Equal(t, 2, snap.getCount())

	// Second batch: all cache hits.
	result, err = cs.BatchGet(ctx, []kv.Key{key1, key2})
	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, 2, snap.getCount()) // no new fetches
}

func TestCachedSnapshotBatchGetAllUncached(t *testing.T) {
	cdb := newTestCacheDBForSnapshot()
	key1 := makeRowKey(2, 100)
	key2 := makeRowKey(3, 200)

	snap := newMockSnapshot(map[string][]byte{
		string(key1): []byte("v1"),
		string(key2): []byte("v2"),
	})

	cs := newCachedSnapshot(snap, cdb, 200, []int64{1}) // only table 1 is cached
	ctx := context.Background()

	result, err := cs.BatchGet(ctx, []kv.Key{key1, key2})
	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, []byte("v1"), result[string(key1)].Value)
	assert.Equal(t, []byte("v2"), result[string(key2)].Value)
}

func TestCachedSnapshotBatchGetWithMissing(t *testing.T) {
	cdb := newTestCacheDBForSnapshot()
	existingKey := makeRowKey(1, 100)
	missingKey := makeRowKey(1, 999)

	snap := newMockSnapshot(map[string][]byte{
		string(existingKey): []byte("exists"),
	})

	cs := newCachedSnapshot(snap, cdb, 200, []int64{1})
	ctx := context.Background()

	result, err := cs.BatchGet(ctx, []kv.Key{existingKey, missingKey})
	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, []byte("exists"), result[string(existingKey)].Value)
}

func TestCachedSnapshotBatchGetMultipleCachedTables(t *testing.T) {
	cdb := newTestCacheDBForSnapshot()
	keyT1 := makeRowKey(1, 100) // cached table 1
	keyT2 := makeRowKey(2, 100) // cached table 2
	keyT3 := makeRowKey(3, 100) // uncached table 3

	snap := newMockSnapshot(map[string][]byte{
		string(keyT1): []byte("t1"),
		string(keyT2): []byte("t2"),
		string(keyT3): []byte("t3"),
	})

	cs := newCachedSnapshot(snap, cdb, 200, []int64{1, 2}) // tables 1 and 2 cached
	ctx := context.Background()

	result, err := cs.BatchGet(ctx, []kv.Key{keyT1, keyT2, keyT3})
	require.NoError(t, err)
	assert.Len(t, result, 3)
	assert.Equal(t, []byte("t1"), result[string(keyT1)].Value)
	assert.Equal(t, []byte("t2"), result[string(keyT2)].Value)
	assert.Equal(t, []byte("t3"), result[string(keyT3)].Value)
}

func TestCachedSnapshotBatchGetEmpty(t *testing.T) {
	cdb := newTestCacheDBForSnapshot()
	snap := newMockSnapshot(map[string][]byte{})

	cs := newCachedSnapshot(snap, cdb, 200, []int64{1})
	ctx := context.Background()

	result, err := cs.BatchGet(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestCachedSnapshotBatchGetWithInvalidation(t *testing.T) {
	cdb := newTestCacheDBForSnapshot()
	key1 := makeRowKey(1, 100)
	key2 := makeRowKey(1, 200)

	snap := newMockSnapshot(map[string][]byte{
		string(key1): []byte("v1"),
		string(key2): []byte("v2"),
	})

	cs := newCachedSnapshot(snap, cdb, 150, []int64{1})
	ctx := context.Background()

	// Populate cache for both keys.
	_, err := cs.BatchGet(ctx, []kv.Key{key1, key2})
	require.NoError(t, err)
	assert.Equal(t, 2, snap.getCount())

	// Invalidate only key1 with min_cached_ts=200.
	cdb.InvalidateKeys(1, []kv.Key{key1}, 200)

	// BatchGet with readTS=150: key1 bypasses cache, key2 served from cache.
	result, err := cs.BatchGet(ctx, []kv.Key{key1, key2})
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), result[string(key1)].Value)
	assert.Equal(t, []byte("v2"), result[string(key2)].Value)
	assert.Equal(t, 3, snap.getCount()) // only key1 fetched again
}

func TestCachedSnapshotNoCachedTables(t *testing.T) {
	cdb := newTestCacheDBForSnapshot()
	key := makeRowKey(1, 100)
	snap := newMockSnapshot(map[string][]byte{
		string(key): []byte("value"),
	})

	// No cached tables — all reads go directly to snapshot.
	cs := newCachedSnapshot(snap, cdb, 200, nil)
	ctx := context.Background()

	entry, err := cs.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), entry.Value)

	// Not cached, so second read goes to snapshot again.
	entry, err = cs.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), entry.Value)
	assert.Equal(t, 2, snap.getCount())
}

func TestCachedSnapshotImplementsInterface(t *testing.T) {
	cdb := newTestCacheDBForSnapshot()
	snap := newMockSnapshot(map[string][]byte{})
	cs := newCachedSnapshot(snap, cdb, 200, []int64{1})

	// Verify that cachedSnapshot satisfies kv.Snapshot.
	var _ kv.Snapshot = cs
}

func TestNewCachedSnapshotTableIDSet(t *testing.T) {
	cdb := newTestCacheDBForSnapshot()
	snap := newMockSnapshot(map[string][]byte{})
	cs := newCachedSnapshot(snap, cdb, 100, []int64{1, 5, 10})

	_, ok := cs.cachedTables[1]
	assert.True(t, ok)
	_, ok = cs.cachedTables[5]
	assert.True(t, ok)
	_, ok = cs.cachedTables[10]
	assert.True(t, ok)
	_, ok = cs.cachedTables[2]
	assert.False(t, ok)
}
