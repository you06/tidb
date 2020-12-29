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
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"sync"
	"unsafe"

	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
)

// deterministicCommitter executes a deterministic commit protocol.
type deterministicCommitter struct {
	store     *tikvStore
	txn       *tikvTxn
	startTS   uint64
	mutations *memBufferMutations
	// commitTS in deterministic transaction means
	commitTS uint64
	priority pb.CommandPri
	connID   uint64 // connID is used for log.
	cleanWg  sync.WaitGroup
	detail   unsafe.Pointer
	txnSize  int

	mu struct {
		sync.RWMutex
		undeterminedErr error // undeterminedErr saves the rpc error we encounter when commit primary key.
		committed       bool
	}
	// regionTxnSize stores the number of keys involved in each region
	regionTxnSize map[uint64]int
}
