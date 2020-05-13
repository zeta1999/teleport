package main

import (
	"strings"
)

type Dialect struct {
	Key             string
	HumanName       string
	TerminalCommand string
}

var (
	mysql    = Dialect{"mysql", "MySQL", ""}
	postgres = Dialect{"postgres", "PostgreSQL", "psql"}
	redshift = Dialect{"redshift", "AWS RedShift", "psql"}
	sqlite   = Dialect{"sqlite", "SQLite3", ""}
)

func DbDialect(c Connection) Dialect {
	if strings.HasPrefix(c.Config.URL, "redshift://") {
		return redshift
	} else if strings.HasPrefix(c.Config.URL, "postgres://") {
		return postgres
	} else if strings.HasPrefix(c.Config.URL, "sqlite://") {
		return sqlite
	}

	return mysql
}
