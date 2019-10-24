// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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

package expression

import (
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

var (
	_ functionClass = &tidbSqlsmithFunctionClass{}
)

type tidbSqlsmithFunctionClass struct {
	baseFunctionClass
}

func (c *tidbSqlsmithFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString)
	bf.tp.Flen = 64
	sig := &tidbSqlsmithSig{bf}
	return sig, nil
}

type tidbSqlsmithSig struct {
	baseBuiltinFunc
}

func (b *tidbSqlsmithSig) Clone() builtinFunc {
	newSig := &builtinTiDBDecodeKeySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a tidbSqlsmithSig.
func (b *tidbSqlsmithSig) evalString(row chunk.Row) (string, bool, error) {
	// s, isNull, err := b.args[0].EvalString(b.ctx, row)
	// if isNull || err != nil {
	// 	return "", isNull, err
	// }
	return "welcome to sqlsmith", false, nil
}
