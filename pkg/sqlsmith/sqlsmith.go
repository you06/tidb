package sqlsmith

import (
	"log"
	sqlsmith_go "github.com/you06/sqlsmith-go"
)

// New create sqlsmith
func New(sql, db string, records [][5]string) string {
	if db == "" {
		return "select a database and try again"
	}

	ss := sqlsmith_go.New()
	ss.LoadSchema(records)
	ss.SetDB(db)

	node := ss.SelectStmt(10)
	sql, err :=	ss.Walk(node)

	if err != nil {
		log.Println(err)
	}

	return sql
}
