package sqlsmith

// SmithSQLType define SQLSmith SQL types
type SmithSQLType int

// SmithSQLTypeMustExec
const (
	SmithSQLTypeMustExec SmithSQLType = iota
	SmithSQLTypeInsert
	SmithSQLTypeSmithSQL
	SmithSQLTypeNotice
	SmithSQLTypeExec
)

// SmithSQL define SQLSmith structure
type SmithSQL struct {
	SQL string
	Type SmithSQLType
	UUID string
}

// GetType get SQL type
func (s *SmithSQL) GetType() SmithSQLType {
	return s.Type
}

// GetSQL get SQL string
func (s *SmithSQL) GetSQL() string {
	return s.SQL
}

// GetSQL get SQL string
func (s *SmithSQL) GetUUID() string {
	return s.UUID
}
