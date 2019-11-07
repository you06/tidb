package sqlsmith

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	sqlsmith_go "github.com/you06/sqlsmith-go"
)

// New create sqlsmith
func New(sql, db string, records [][5]string, sqlCh chan *SmithSQL) {
	commands := strings.Split(sql, " ")
	if len(commands) == 1 {
		New(fmt.Sprintf("%s exec 1", sql), db, records, sqlCh)
		return
	}
	defer close(sqlCh)
	if db == "" {
		sqlCh <- &SmithSQL{
			SQL: "select a database and try again",
			Type: SmithSQLTypeNotice,
		}
		return
	}
	sqlCh <- &SmithSQL{
		SQL: fmt.Sprintf("use %s", db),
		Type: SmithSQLTypeMustExec,
	}
	start := time.Now()
	ss := sqlsmith_go.New()
	ss.LoadSchema(records)
	ss.SetDB(db)

	switch commands[1] {
	case "insert": {
		if len(commands) == 2 {
			insertData(ss, 10, 10, sqlCh)
			return
		}
		count, err := strconv.Atoi(commands[2])
		if err != nil {
			sqlCh <- &SmithSQL{
				SQL: "2nd arg must be an int",
				Type: SmithSQLTypeNotice,
			}
			return
		}
		if len(commands) == 3 {
			insertData(ss, count, 10, sqlCh)
			return
		}
		depth, err := strconv.Atoi(commands[2])
		if err != nil {
			sqlCh <- &SmithSQL{
				SQL: "3rd arg must be an int",
				Type: SmithSQLTypeNotice,
			}
			return
		}
		insertData(ss, count, depth, sqlCh)
	}
	case "exec": {
		if len(commands) == 2 {
			exec(ss, 1, 1, sqlCh)
			return
		}
		depth, err := strconv.Atoi(commands[2])
		if err != nil {
			sqlCh <- &SmithSQL{
				SQL: "2nd arg must be an int",
				Type: SmithSQLTypeNotice,
			}
			return
		}
		if len(commands) == 3 {
			exec(ss, depth, 1, sqlCh)
			return
		}
		count, err := strconv.Atoi(commands[3])
		if err != nil {
			sqlCh <- &SmithSQL{
				SQL: "3rd arg must be an int",
				Type: SmithSQLTypeNotice,
			}
			return
		}
		exec(ss, depth, count, sqlCh)
	}
	case "log": {
		logSQL := "select * from test.sqlsmith order by created_at asc limit %d"
		if len(commands) == 2 {
			sqlCh <- &SmithSQL{
				SQL: fmt.Sprintf(logSQL, 5),
				Type: SmithSQLTypeExec,
			}
			return
		}
		limit, err := strconv.Atoi(commands[2])
		if err != nil {
			sqlCh <- &SmithSQL{
				SQL: "2nd arg must be an int",
				Type: SmithSQLTypeNotice,
			}
			return
		}
		sqlCh <- &SmithSQL{
			SQL: fmt.Sprintf(logSQL, limit),
			Type: SmithSQLTypeExec,
		}
	}
	}

	duration := int(time.Now().Sub(start).Seconds())
	sqlCh <- &SmithSQL{
		SQL: fmt.Sprintf("exec success in %d seconds.", duration),
		Type: SmithSQLTypeNotice,
	}
}
