package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetDialect(t *testing.T) {
	cases := []struct {
		URL             string
		expectedDialect Dialect
	}{
		{
			"postgres://user:pass@test.host/dbname",
			postgres,
		},
		{
			"postgresql://user:pass@test.host/dbname",
			postgres,
		},
		{
			"pg://user:pass@test.host/dbname",
			postgres,
		},
		{
			"mysql://user:pass@test.host/dbname",
			mysql,
		},
		{
			"mariadb://user:pass@test.host/dbname",
			mysql,
		},
		{
			"sqlite://tmp/db.sqlite3",
			sqlite,
		},
		{
			"sqlite3://tmp/db.sqlite3",
			sqlite,
		},
		{
			"redshift://user:pass@test.host/dbname",
			redshift,
		},
		{
			"rs://user:pass@test.host/dbname",
			redshift,
		},

		// Not yet supported
		{
			"snowflake://user:pass@test.host/dbname",
			mysql, // Default
		},
		{
			"sqlserver://user:pass@test.host/dbname",
			mysql, // Default
		},
	}

	for _, cse := range cases {
		actualDialect := GetDialect(Database{cse.URL, map[string]string{}, false})
		assert.Equal(t, cse.expectedDialect, actualDialect)

	}
}
