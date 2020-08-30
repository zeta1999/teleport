package main

import (
	"strings"

	"github.com/xo/dburl"
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
		"CREATE TABLE %[2]s AS TABLE %[1]s WITH NO DATA",
		`
			ALTER TABLE %[1]s RENAME TO archive_%[1]s;
			ALTER TABLE %[2]s RENAME TO %[1]s;
			DROP TABLE archive_%[1]s;
		`,
		`
			DELETE FROM %[1]s WHERE %[3]s IN (SELECT %[3]s FROM %[2]s);
			INSERT INTO %[1]s SELECT * FROM %[2]s;
			DROP TABLE %[2]s
		`}
	redshift = Dialect{"redshift", "AWS RedShift", "psql",
		"CREATE TEMPORARY TABLE %[2]s (LIKE %[1]s)",
		`
			BEGIN TRANSACTION;
			DELETE FROM %[1]s;
			INSERT INTO %[1]s SELECT * FROM %[2]s;
			END TRANSACTION;
		`,
		`
			DELETE FROM %[1]s USING %[2]s WHERE %[1]s.%[3]s = %[2]s.%[3]s;
			INSERT INTO %[1]s SELECT * FROM %[2]s;
		`}
	sqlite = Dialect{"sqlite", "SQLite3", "",
		"CREATE TABLE %[2]s AS SELECT * FROM %[1]s LIMIT 0", postgres.FullLoadQuery, postgres.ModifiedOnlyLoadQuery}
)

func GetDialect(d Database) Dialect {
	if strings.HasPrefix(d.URL, "redshift://") {
		return redshift
	} else if strings.HasPrefix(d.URL, "rs://") {
		return redshift
	}

	if u, err := dburl.Parse(d.URL); err == nil {
		switch u.Driver {
		case "postgres":
			return postgres
		case "sqlite3":
			return sqlite
		case "mysql":
			return mysql
		default:
			return mysql
		}
	}

	return mysql
}
