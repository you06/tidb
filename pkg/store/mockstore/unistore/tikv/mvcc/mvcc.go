// Copyright 2019-present PingCAP, Inc.
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

package mvcc

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"sort"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/util/codec"
)

var defaultEndian = binary.LittleEndian

// DBUserMeta is the user meta used in DB.
type DBUserMeta []byte

// DecodeLock decodes data to lock, the primary and value is copied, the secondaries are copied if async commit is enabled.
func DecodeLock(data []byte) (l Lock) {
	l.LockHdr = *(*LockHdr)(unsafe.Pointer(&data[0]))
	cursor := mvccLockHdrSize
	lockBuf := slices.Clone(data[cursor:])
	l.Primary = lockBuf[:l.PrimaryLen]
	cursor = int(l.PrimaryLen)
	if l.LockHdr.SecondaryNum > 0 {
		l.Secondaries = make([][]byte, l.LockHdr.SecondaryNum)
		for i := range l.LockHdr.SecondaryNum {
			keyLen := binary.LittleEndian.Uint16(lockBuf[cursor:])
			cursor += 2
			l.Secondaries[i] = lockBuf[cursor : cursor+int(keyLen)]
			cursor += int(keyLen)
		}
	}
	l.Value = lockBuf[cursor:]
	if kvrpcpb.Op(l.Op) == kvrpcpb.Op_SharedLock {
		l.SharedLocks = decodeSharedLocks(l.Value)
		l.Value = nil
	}
	return
}

// LockHdr holds fixed size fields for mvcc Lock.
type LockHdr struct {
	StartTS        uint64
	ForUpdateTS    uint64
	MinCommitTS    uint64
	TTL            uint32
	Op             uint8
	HasOldVer      bool
	PrimaryLen     uint16
	UseAsyncCommit bool
	SecondaryNum   uint32
}

const mvccLockHdrSize = int(unsafe.Sizeof(LockHdr{}))

// Lock is the structure for MVCC lock.
type Lock struct {
	LockHdr
	Primary     []byte
	Value       []byte
	Secondaries [][]byte
	// SharedLocks is only used when Op is `kvrpcpb.Op_SharedLock`.
	// It stores per-transaction locks keyed by their StartTS.
	SharedLocks map[uint64]Lock
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (l *Lock) MarshalBinary() []byte {
	valueToEncode := l.Value
	primaryToEncode := l.Primary
	secondariesToEncode := l.Secondaries
	lockHdr := l.LockHdr
	if kvrpcpb.Op(lockHdr.Op) == kvrpcpb.Op_SharedLock {
		primaryToEncode = nil
		secondariesToEncode = nil
		valueToEncode = encodeSharedLocks(l.SharedLocks)
		lockHdr.PrimaryLen = 0
		lockHdr.SecondaryNum = 0
		lockHdr.UseAsyncCommit = false
		lockHdr.HasOldVer = false
		lockHdr.TTL = 0
		lockHdr.MinCommitTS = 0
	}

	lockLen := mvccLockHdrSize + len(primaryToEncode) + len(valueToEncode)
	length := lockLen
	if lockHdr.SecondaryNum > 0 {
		for _, secondaryKey := range secondariesToEncode {
			length += 2
			length += len(secondaryKey)
		}
	}
	buf := make([]byte, length)
	hdr := (*LockHdr)(unsafe.Pointer(&buf[0]))
	*hdr = lockHdr
	cursor := mvccLockHdrSize
	copy(buf[cursor:], primaryToEncode)
	cursor += len(primaryToEncode)
	if lockHdr.SecondaryNum > 0 {
		for _, secondaryKey := range secondariesToEncode {
			binary.LittleEndian.PutUint16(buf[cursor:], uint16(len(secondaryKey)))
			cursor += 2
			copy(buf[cursor:], secondaryKey)
			cursor += len(secondaryKey)
		}
	}
	copy(buf[cursor:], valueToEncode)
	return buf
}

// ToLockInfo converts an mvcc Lock to kvrpcpb.LockInfo
func (l *Lock) ToLockInfo(key []byte) *kvrpcpb.LockInfo {
	if kvrpcpb.Op(l.Op) == kvrpcpb.Op_SharedLock {
		info := &kvrpcpb.LockInfo{
			Key:      key,
			LockType: kvrpcpb.Op_SharedLock,
		}

		if len(l.SharedLocks) == 0 {
			return info
		}

		startTSList := make([]uint64, 0, len(l.SharedLocks))
		for ts := range l.SharedLocks {
			startTSList = append(startTSList, ts)
		}
		sort.Slice(startTSList, func(i, j int) bool { return startTSList[i] < startTSList[j] })
		info.SharedLockInfos = make([]*kvrpcpb.LockInfo, 0, len(startTSList))
		for _, startTS := range startTSList {
			sub := l.SharedLocks[startTS]
			subInfo := sub.ToLockInfo(key)
			switch subInfo.LockType {
			case kvrpcpb.Op_Lock:
				subInfo.LockType = kvrpcpb.Op_SharedLock
			case kvrpcpb.Op_PessimisticLock:
				subInfo.LockType = kvrpcpb.Op_SharedPessimisticLock
			default:
				// Shared locks should only contain lock/pessimistic entries.
				subInfo.LockType = kvrpcpb.Op_SharedLock
			}
			subInfo.SharedLockInfos = nil
			info.SharedLockInfos = append(info.SharedLockInfos, subInfo)
		}
		return info
	}

	return &kvrpcpb.LockInfo{
		PrimaryLock:     l.Primary,
		LockVersion:     l.StartTS,
		Key:             key,
		LockTtl:         uint64(l.TTL),
		LockType:        kvrpcpb.Op(l.Op),
		LockForUpdateTs: l.ForUpdateTS,
		UseAsyncCommit:  l.UseAsyncCommit,
		MinCommitTs:     l.MinCommitTS,
		Secondaries:     l.Secondaries,
	}
}

// String implements fmt.Stringer for Lock.
func (l *Lock) String() string {
	return fmt.Sprintf(
		"Lock { Type: %v, StartTS: %v,  ForUpdateTS: %v, Primary: %v, UseAsyncCommit: %v }",
		kvrpcpb.Op(l.Op).String(),
		l.StartTS,
		l.ForUpdateTS,
		hex.EncodeToString(l.Primary),
		l.UseAsyncCommit,
	)
}

func encodeSharedLocks(locks map[uint64]Lock) []byte {
	if len(locks) == 0 {
		return make([]byte, 4)
	}
	startTSList := make([]uint64, 0, len(locks))
	for ts := range locks {
		startTSList = append(startTSList, ts)
	}
	sort.Slice(startTSList, func(i, j int) bool { return startTSList[i] < startTSList[j] })

	total := 4
	encoded := make([][]byte, 0, len(startTSList))
	for _, ts := range startTSList {
		sub := locks[ts]
		b := sub.MarshalBinary()
		encoded = append(encoded, b)
		total += 4 + len(b)
	}

	buf := make([]byte, total)
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(encoded)))
	cursor := 4
	for _, b := range encoded {
		binary.LittleEndian.PutUint32(buf[cursor:cursor+4], uint32(len(b)))
		cursor += 4
		copy(buf[cursor:], b)
		cursor += len(b)
	}
	return buf
}

func decodeSharedLocks(data []byte) map[uint64]Lock {
	if len(data) < 4 {
		return map[uint64]Lock{}
	}
	n := int(binary.LittleEndian.Uint32(data[:4]))
	cursor := 4
	out := make(map[uint64]Lock, n)
	for range n {
		if cursor+4 > len(data) {
			break
		}
		l := int(binary.LittleEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4
		if l < 0 || cursor+l > len(data) {
			break
		}
		sub := DecodeLock(data[cursor : cursor+l])
		cursor += l
		out[sub.StartTS] = sub
	}
	return out
}

func (l *Lock) IsSharedLock() bool {
	return kvrpcpb.Op(l.Op) == kvrpcpb.Op_SharedLock
}

func (l *Lock) ContainsStartTS(startTS uint64) bool {
	if !l.IsSharedLock() {
		return l.StartTS == startTS
	}
	if l.SharedLocks == nil {
		return false
	}
	_, ok := l.SharedLocks[startTS]
	return ok
}

func (l *Lock) GetSharedLock(startTS uint64) (Lock, bool) {
	if !l.IsSharedLock() || l.SharedLocks == nil {
		return Lock{}, false
	}
	sub, ok := l.SharedLocks[startTS]
	return sub, ok
}

func (l *Lock) RemoveSharedLock(startTS uint64) (Lock, bool) {
	if !l.IsSharedLock() || l.SharedLocks == nil {
		return Lock{}, false
	}
	sub, ok := l.SharedLocks[startTS]
	if !ok {
		return Lock{}, false
	}
	delete(l.SharedLocks, startTS)
	l.recomputeSharedMinTS()
	return sub, true
}

func (l *Lock) PutSharedLock(sub Lock) {
	if l.SharedLocks == nil {
		l.SharedLocks = make(map[uint64]Lock)
	}
	_, replaced := l.SharedLocks[sub.StartTS]
	l.SharedLocks[sub.StartTS] = sub
	if replaced {
		l.recomputeSharedMinTS()
		return
	}
	if l.StartTS == 0 {
		l.StartTS = math.MaxUint64
	}
	if l.ForUpdateTS == 0 {
		l.ForUpdateTS = math.MaxUint64
	}
	l.StartTS = min(l.StartTS, sub.StartTS)
	if sub.ForUpdateTS > 0 {
		l.ForUpdateTS = min(l.ForUpdateTS, sub.ForUpdateTS)
	}
}

func (l *Lock) recomputeSharedMinTS() {
	if !l.IsSharedLock() {
		return
	}
	if len(l.SharedLocks) == 0 {
		l.StartTS = 0
		l.ForUpdateTS = 0
		return
	}
	minStart := uint64(math.MaxUint64)
	minForUpdate := uint64(math.MaxUint64)
	for _, sub := range l.SharedLocks {
		minStart = min(minStart, sub.StartTS)
		if sub.ForUpdateTS > 0 {
			minForUpdate = min(minForUpdate, sub.ForUpdateTS)
		}
	}
	l.StartTS = minStart
	if minForUpdate == uint64(math.MaxUint64) {
		l.ForUpdateTS = 0
	} else {
		l.ForUpdateTS = minForUpdate
	}
}

// UserMeta value for lock.
const (
	LockUserMetaNoneByte   = 0
	LockUserMetaDeleteByte = 2
)

// UserMeta byte slices for lock.
var (
	LockUserMetaNone   = []byte{LockUserMetaNoneByte}
	LockUserMetaDelete = []byte{LockUserMetaDeleteByte}
)

// DecodeKeyTS decodes the TS in a key.
func DecodeKeyTS(buf []byte) uint64 {
	tsBin := buf[len(buf)-8:]
	_, ts, err := codec.DecodeUintDesc(tsBin)
	if err != nil {
		panic(err)
	}
	return ts
}

// NewDBUserMeta creates a new DBUserMeta.
func NewDBUserMeta(startTS, commitTS uint64) DBUserMeta {
	m := make(DBUserMeta, 16)
	defaultEndian.PutUint64(m, startTS)
	defaultEndian.PutUint64(m[8:], commitTS)
	return m
}

// CommitTS reads the commitTS from the DBUserMeta.
func (m DBUserMeta) CommitTS() uint64 {
	return defaultEndian.Uint64(m[8:])
}

// StartTS reads the startTS from the DBUserMeta.
func (m DBUserMeta) StartTS() uint64 {
	return defaultEndian.Uint64(m[:8])
}

// EncodeExtraTxnStatusKey encodes a extra transaction status key.
// It is only used for Rollback and Op_Lock.
func EncodeExtraTxnStatusKey(key []byte, startTS uint64) []byte {
	b := slices.Clone(key)
	ret := codec.EncodeUintDesc(b, startTS)
	ret[0]++
	return ret
}

// DecodeExtraTxnStatusKey decodes a extra transaction status key.
func DecodeExtraTxnStatusKey(extraKey []byte) (key []byte) {
	if len(extraKey) <= 9 {
		return nil
	}
	key = slices.Clone(extraKey[:len(extraKey)-8])
	key[0]--
	return
}
