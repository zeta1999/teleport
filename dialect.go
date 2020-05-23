package main

import (
	"strings"
)

type Dialect struct {
	Key                      string
	HumanName                string
	TerminalCommand          string
	CreateStagingTableQuery  string
	PromoteStagingTableQuery string
}

var (
	mysql    = Dialect{"mysql", "MySQL", "", "", ""}
	postgres = Dialect{"postgres", "PostgreSQL", "psql",
		"CREATE TABLE staging_%[1]s AS TABLE %[1]s WITH NO DATA",
		`
			ALTER TABLE %[1]s RENAME TO old_%[1]s;
			ALTER TABLE staging_%[1]s RENAME TO %[1]s;
			DROP TABLE old_%[1]s;
		`}
	redshift = Dialect{"redshift", "AWS RedShift", "psql",
		"CREATE TEMPORARY TABLE staging_%[1]s (LIKE %[1]s)",
		`
			BEGIN TRANSACTION;
			DELETE FROM %[1]s;
			INSERT INTO %[1]s SELECT * FROM staging_%[1]s;
			END TRANSACTION;
		`}
	sqlite = Dialect{"sqlite", "SQLite3", "",
		"CREATE TABLE staging_%[1]s AS SELECT * FROM %[1]s LIMIT 0", postgres.PromoteStagingTableQuery}
)

func GetDialect(c Connection) Dialect {
	if strings.HasPrefix(c.Config.URL, "redshift://") {
		return redshift
	} else if strings.HasPrefix(c.Config.URL, "postgres://") {
		return postgres
	} else if strings.HasPrefix(c.Config.URL, "sqlite://") {
		return sqlite
	}

	return mysql
}
