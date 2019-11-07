package sqlsmith

import (
	"fmt"
	sqlsmith_go "github.com/you06/sqlsmith-go"
)

func insertData(ss *sqlsmith_go.SQLSmith, count, batchSize int, sqlCh chan *SmithSQL) {
	gen, err := ss.GenData(count, batchSize)
	if err != nil {
		sqlCh <- &SmithSQL{
			SQL: fmt.Sprintf("generate sql error %v", err),
			Type: SmithSQLTypeNotice,
		}
		return
	}

	for sqls := gen.Next(); len(sqls) != 0; sqls = gen.Next() {
		for _, sql := range sqls {
			fmt.Println("sql insert", sql)
			sqlCh <- &SmithSQL{
				SQL: sql,
				Type: SmithSQLTypeInsert,
			}
		}
	}
}
