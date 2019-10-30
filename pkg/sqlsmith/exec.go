package sqlsmith

import (
	"fmt"
	sqlsmith_go "github.com/you06/sqlsmith-go"
	"github.com/google/uuid"
)

func exec(ss *sqlsmith_go.SQLSmith, depth, count int, sqlCh chan *SmithSQL) {
	// sqlCh <- &SmithSQL{
	// 	SQL: SmithTable,
	// 	Type: SmithSQLTypeMustExec,
	// }
	// sqlCh <- &SmithSQL{
	// 	SQL: SessionSQLMode,
	// 	Type: SmithSQLTypeMustExec,
	// }

	for i := 0; i < count; i++ {
		node := ss.SelectStmt(depth)
		sql, err :=	ss.Walk(node)

		if err != nil {
			sqlCh <- &SmithSQL{
				SQL: fmt.Sprintf("generate SQLSmith SQL error %v", err),
				Type: SmithSQLTypeSmithSQL,
			}
			return
		}
		sqlCh <- &SmithSQL{
			SQL: sql,
			Type: SmithSQLTypeSmithSQL,
			UUID: uuid.New().String(),
		}
	}

	sqlCh <- &SmithSQL{
		SQL: fmt.Sprintf("%d SQLs with depth %d exec finished", count, depth),
		Type: SmithSQLTypeNotice,
	}
}
