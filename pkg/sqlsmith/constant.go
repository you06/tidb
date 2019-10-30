package sqlsmith

const (
	// SQLs
	
	// SessionSQLMode set SQL mode
	SessionSQLMode = `SET SESSION sql_mode = "NO_ENGINE_SUBSTITUTION"`
	// SmithTable is SQLSmith log storage
	SmithTable = "CREATE TABLE IF NOT EXISTS test.sqlsmith (" +
		"`id` int(11) NOT NULL AUTO_INCREMENT," +
		"`uuid` varchar(1023) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL," +
		"`sql` text COLLATE utf8mb4_general_ci," +
		"`db` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL," +
		"`status` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL," +
		"`created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP," +
		"`duration` int(11) DEFAULT '0'," +
		"	PRIMARY KEY (`id`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
	// ProcessSQL is for create SQLSmith in process
	ProcessSQL = "INSERT INTO test.sqlsmith(`uuid`, `sql`, `db`, `status`) VALUES(\"%s\", \"%s\", \"%s\", \"process\")"
	// SuccessSQL is for create SQLSmith in process
	SuccessSQL = "UPDATE test.sqlsmith SET status=\"success\", duration=%d WHERE uuid=\"%s\""
	// FailSQL is for create SQLSmith in process
	FailSQL = "UPDATE test.sqlsmith SET status=\"failed\", duration=%d WHERE uuid=\"%s\""
)
