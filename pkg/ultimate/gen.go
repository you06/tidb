package ultimate

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"

)

func GenCreateTable(columnName int) (sql string, tableName string, columnsType []string) {
	var (
		columnTypes = []string{
			"TINYINT",
			"INT",
			"BIGINT",
			"CHAR(32)",
			"CHAR(256)",
			"VARCHAR(32)",
			"VARCHAR(256)",
			"TEXT",
			"BLOB",
			"DATETIME",
			"TIMESTAMP",
		}
		sqlBuf bytes.Buffer
	)
	tableName = uuid.New().String()
	sqlBuf.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXIST `%s` ( id bigint, ", tableName))
	for i := 0; i < columnName; i++ {
		columnType := columnTypes[rand.Intn(len(columnTypes))]
		columnsType = append(columnsType, columnType)
		c := fmt.Sprintf(" %s %s,", fmt.Sprintf("column%d", i), columnType)
		sqlBuf.WriteString(c)
	}
	sqlBuf.WriteString(fmt.Sprintf("PRIMARY KEY id)CHARSET=utf8mb4;"))
	return sqlBuf.String(), tableName, columnsType
}

type fakeDataGen func() interface{}

func GenInsertTable(tableName string, columnsType []string) string {
	var (
		genSet = map[string]fakeDataGen{
			"TINYINT": func() interface{} {
				return rand.Intn(255) - 128
			},
			"INT": func() interface{} {
				return rand.Int31()
			},
			"BIGINT": func() interface{} {
				return rand.Int63()
			},
			"CHAR(32)": func() interface{} {
				return RandStringBytesMaskImpr(rand.Intn(32))
			},
			"CHAR(256)": func() interface{} {
				return RandStringBytesMaskImpr(rand.Intn(256))
			},
			"VARCHAR(32)": func() interface{} {
				return RandStringBytesMaskImpr(rand.Intn(32))
			},
			"VARCHAR(256)": func() interface{} {
				return RandStringBytesMaskImpr(rand.Intn(32))
			},
			"TEXT": func() interface{} {
				return RandStringBytesMaskImpr(rand.Intn(10000))
			},
			"BLOB": func() interface{} {
				return RandStringBytesMaskImpr(rand.Intn(10000))
			},
			"DATETIME": func() interface{} {
				return time.Now().Format("2016-01-02 15:04:05")
			},
			"TIMESTAMP": func() interface{} {
				return time.Now().Unix()
			},
		}
		sqlBuf bytes.Buffer
	)
	sqlBuf.WriteString(fmt.Sprintf("INSERT INTO %s VALUES(", tableName))
	for _, v := range columnsType {
		c := fmt.Sprintf(" %v, ", genSet[v]())
		sqlBuf.WriteString(c)
	}
	sqlBuf.WriteString(fmt.Sprintf(")"))
	return sqlBuf.String()
}

func GenDropTable(tableName string) string {
	return fmt.Sprintf("DROP TABLE %s", tableName)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandStringBytesMaskImpr(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}