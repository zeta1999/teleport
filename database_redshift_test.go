// +build redshift

package main

import (
	"database/sql"
	"os"
	"testing"

	"github.com/hundredwatt/teleport/schema"
	"github.com/stretchr/testify/assert"
)

func TestRedshiftCreateTableText(t *testing.T) {
	withRedshift(t, func(t *testing.T, srcdb *sql.DB, destdb *sql.DB) {
		objects := schema.Table{"", "objects", make([]schema.Column, 1)}
		objects.Columns[0] = schema.Column{"description", schema.TEXT, map[schema.Option]int{}}

		createTable("testsrc", "objects", &objects)
		defer srcdb.Exec(`DROP TABLE IF EXISTS objects`)

		table, _ := schema.DumpTableMetadata(srcdb, "objects")

		assert.Equal(t, schema.Column{"description", schema.STRING, map[schema.Option]int{schema.LENGTH: 65535}}, table.Columns[0])
	})
}

func withRedshift(t *testing.T, testfn func(*testing.T, *sql.DB, *sql.DB)) {
	redshiftURL := os.ExpandEnv("redshift://$TEST_REDSHIFT_USER:$TEST_REDSHIFT_PASSWORD@$TEST_REDSHIFT_HOST:5439/dev")

	Databases["testsrc"] = Database{redshiftURL, map[string]string{}, true}
	srcdb, err := connectDatabase("testsrc")
	if err != nil {
		assert.FailNow(t, "%w", err)
	}
	defer delete(dbs, "testsrc")

	Databases["testdest"] = Database{"postgres://postgres@localhost:45432/?sslmode=disable", map[string]string{}, false}
	destdb, err := connectDatabase("testdest")
	if err != nil {
		assert.FailNow(t, "%w", err)
	}
	defer delete(dbs, "testdest")

	defer srcdb.Exec(`DROP TABLE IF EXISTS widgets`)
	defer destdb.Exec(`DROP TABLE IF EXISTS testsrc_widgets`)

	testfn(t, srcdb, destdb)
}
