package main

import (
	"strings"
)

type Dialect struct {
	Key                     string
	HumanName               string
	TerminalCommand         string
	CreateStagingTableQuery string
	FullLoadQuery           string
	ModifiedOnlyLoadQuery   string
}

var (
	mysql    = Dialect{"mysql", "MySQL", "", "", "", ""}
	postgres = Dialect{"postgres", "PostgreSQL", "psql",
		"CREATE TABLE staging_%[1]s AS TABLE %[1]s WITH NO DATA",
		`
			ALTER TABLE %[1]s RENAME TO old_%[1]s;
			ALTER TABLE staging_%[1]s RENAME TO %[1]s;
			DROP TABLE old_%[1]s;
		`,
		`
			DELETE FROM %[1]s WHERE %[2]s IN (SELECT %[2]s FROM staging_%[1]s);
			INSERT INTO %[1]s SELECT * FROM staging_%[1]s;
			DROP TABLE staging_%[1]s
		`}
	redshift = Dialect{"redshift", "AWS RedShift", "psql",
		"CREATE TEMPORARY TABLE staging_%[1]s (LIKE %[1]s)",
		`
			BEGIN TRANSACTION;
			DELETE FROM %[1]s;
			INSERT INTO %[1]s SELECT * FROM staging_%[1]s;
			END TRANSACTION;
		`,
		`
			DELETE FROM %[1]s USING staging_%[1]s WHERE %[1]s.%[2]s = staging_%[1]s.%[2]s;
			INSERT INTO %[1]s SELECT * FROM staging_%[1]s;
		`}
	sqlite = Dialect{"sqlite", "SQLite3", "",
		"CREATE TABLE staging_%[1]s AS SELECT * FROM %[1]s LIMIT 0", postgres.FullLoadQuery, postgres.ModifiedOnlyLoadQuery}
)

func GetDialect(d Database) Dialect {
	if strings.HasPrefix(d.URL, "redshift://") {
		return redshift
	} else if strings.HasPrefix(d.URL, "postgres://") {
		return postgres
	} else if strings.HasPrefix(d.URL, "sqlite://") {
		return sqlite
	}

	return mysql
}
