package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostgreLoadExtractLoadConsistency(t *testing.T) {
	Databases["testsrc"] = Database{"postgres://postgres@localhost:45432/?sslmode=disable", map[string]string{}, true}
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

	srcdb.Exec(srcdb.GenerateCreateTableStatement("widgets", widgetsTableDefinition))

	redirectLogs(t, func() {
		err = importCSV("testsrc", "widgets", "testdata/example_widgets.csv", widgetsTableDefinition.Columns)
		assert.NoError(t, err)
		extractLoadDatabase("testsrc", "testdest", "widgets")

		newTable, err := destdb.DumpTableMetadata("testsrc_widgets")
		assert.NoError(t, err)
		assertRowCount(t, 10, destdb, "testsrc_widgets")
		assert.Equal(t, widgetsTableDefinition.Columns, newTable.Columns)
	})
}

func TestPostgresWithSchema(t *testing.T) {
	Databases["pg"] = Database{"postgres://postgres@localhost:45432/?sslmode=disable", map[string]string{}, true}
	pg, err := connectDatabase("pg")
	assert.NoError(t, err)
	defer delete(dbs, "pg")

	_, err = pg.Exec("CREATE SCHEMA a")
	assert.NoError(t, err)
	defer pg.Exec("DROP SCHEMA a")

	Databases["pg_a"] = Database{"postgres://postgres@localhost:45432/?sslmode=disable", map[string]string{"schema": "a"}, true}
	pgA, err := connectDatabase("pg_a")
	assert.NoError(t, err)
	defer delete(dbs, "pg_a")

	_, err = pg.Exec("CREATE TABLE table1 (id INT8)")
	defer pg.Exec("DROP TABLE table1")
	exists, _ := tableExists("pg", "table1")
	assert.True(t, exists)
	exists, _ = tableExists("pg_a", "table1")
	assert.False(t, exists)

	_, err = pgA.Exec("CREATE TABLE table2 (id INT8)")
	defer pgA.Exec("DROP TABLE table2")
	exists, _ = tableExists("pg", "table2")
	assert.False(t, exists)
	exists, _ = tableExists("pg_a", "table2")
	assert.True(t, exists)
}
