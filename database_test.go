package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/hundredwatt/teleport/schema"
	"github.com/stretchr/testify/assert"
)

var (
	widgetsTableDefinition = readTableFromConfigFile("test/example_widgets.yml")
)

func TestLoadNewTable(t *testing.T) {
	runDatabaseTest(t, func(t *testing.T, srcdb *sql.DB, destdb *sql.DB) {
		srcdb.Exec(widgetsTableDefinition.GenerateCreateTableStatement("widgets"))
		importCSV("testsrc", "widgets", "test/example_widgets.csv", widgetsTableDefinition.Columns)

		redirectLogs(t, func() {
			extractLoadDatabase("testsrc", "testdest", "widgets", fullStrategyOpts)

			assertRowCount(t, 10, destdb, "testsrc_widgets")
		})
	})
}

// Skip this test until schema for SQLite staging tables is fixed
func xTestSQLiteLoadExtractLoadConsistency(t *testing.T) {
	runDatabaseTest(t, func(t *testing.T, srcdb *sql.DB, destdb *sql.DB) {
		srcdb.Exec(widgetsTableDefinition.GenerateCreateTableStatement("widgets"))
		redirectLogs(t, func() {
			err := importCSV("testsrc", "widgets", "test/example_widgets.csv", widgetsTableDefinition.Columns)
			assert.NoError(t, err)
			extractLoadDatabase("testsrc", "testdest", "widgets", fullStrategyOpts)

			newTable, err := schema.DumpTableMetadata(destdb, "testsrc_widgets")
			assert.NoError(t, err)
			assertRowCount(t, 10, destdb, "testsrc_widgets")
			assert.Equal(t, widgetsTableDefinition.Columns, newTable.Columns)
		})
	})
}

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

	srcdb.Exec(widgetsTableDefinition.GenerateCreateTableStatement("widgets"))
	redirectLogs(t, func() {
		err := importCSV("testsrc", "widgets", "test/example_widgets.csv", widgetsTableDefinition.Columns)
		assert.NoError(t, err)
		extractLoadDatabase("testsrc", "testdest", "widgets", fullStrategyOpts)

		newTable, err := schema.DumpTableMetadata(destdb, "testsrc_widgets")
		assert.NoError(t, err)
		assertRowCount(t, 10, destdb, "testsrc_widgets")
		assert.Equal(t, widgetsTableDefinition.Columns, newTable.Columns)
	})
}

func TestLoadSourceHasAdditionalColumn(t *testing.T) {
	runDatabaseTest(t, func(t *testing.T, srcdb *sql.DB, destdb *sql.DB) {
		// Create a new schema.Table Definition, same as widgets, but without the `description` column
		widgetsWithoutDescription := schema.Table{"example", "widgets", make([]schema.Column, 0)}
		widgetsWithoutDescription.Columns = append(widgetsWithoutDescription.Columns, widgetsTableDefinition.Columns[:2]...)
		widgetsWithoutDescription.Columns = append(widgetsWithoutDescription.Columns, widgetsTableDefinition.Columns[3:]...)

		srcdb.Exec(widgetsTableDefinition.GenerateCreateTableStatement("widgets"))
		destdb.Exec(widgetsWithoutDescription.GenerateCreateTableStatement("testsrc_widgets"))
		importCSV("testsrc", "widgets", "test/example_widgets.csv", widgetsTableDefinition.Columns)

		expectLogMessages(t, []string{"destination table does not define column", "ranking"}, func() {
			extractLoadDatabase("testsrc", "testdest", "widgets", fullStrategyOpts)

			assertRowCount(t, 10, destdb, "testsrc_widgets")
		})
	})
}

func TestLoadStringNotLongEnough(t *testing.T) {
	runDatabaseTest(t, func(t *testing.T, srcdb *sql.DB, destdb *sql.DB) {
		// Create a new schema.Table Definition, same as widgets, but with name LENGTH changed to 32
		widgetsWithShortName := schema.Table{"example", "widgets", make([]schema.Column, len(widgetsTableDefinition.Columns))}
		copy(widgetsWithShortName.Columns, widgetsTableDefinition.Columns)
		widgetsWithShortName.Columns[3] = schema.Column{"name", schema.STRING, map[schema.Option]int{schema.LENGTH: 32}}

		srcdb.Exec(widgetsTableDefinition.GenerateCreateTableStatement("widgets"))
		destdb.Exec(widgetsWithShortName.GenerateCreateTableStatement("testsrc_widgets"))

		expectLogMessage(t, "For string column `name`, destination LENGTH is too short", func() {
			extractLoadDatabase("testsrc", "testdest", "widgets", fullStrategyOpts)
		})
	})
}

func TestModifiedOnlyStrategy(t *testing.T) {
	runDatabaseTest(t, func(t *testing.T, srcdb *sql.DB, destdb *sql.DB) {
		setupObjectsTable(srcdb)

		redirectLogs(t, func() {
			strategyOpts := StrategyOptions{"modified-only", "id", "updated_at", "36"}
			extractLoadDatabase("testsrc", "testdest", "objects", strategyOpts)

			assertRowCount(t, 2, destdb, "testsrc_objects")
		})
	})
}

func TestExportTimestamp(t *testing.T) {
	runDatabaseTest(t, func(t *testing.T, db *sql.DB, _ *sql.DB) {
		columns := make([]schema.Column, 0)
		columns = append(columns, schema.Column{"created_at", schema.TIMESTAMP, map[schema.Option]int{}})
		table := schema.Table{"test1", "timestamps", columns}

		db.Exec(table.GenerateCreateTableStatement("timestamps"))
		db.Exec("INSERT INTO timestamps (created_at) VALUES (DATETIME(1092941466, 'unixepoch'))")
		db.Exec("INSERT INTO timestamps (created_at) VALUES (NULL)")

		redirectLogs(t, func() {
			tempfile, _ := exportCSV("testsrc", "timestamps", columns, "")

			assertCsvCellContents(t, "2004-08-19T18:51:06Z", tempfile, 0, 0)
			assertCsvCellContents(t, "", tempfile, 1, 0)
		})
	})
}

func TestDatabasePreview(t *testing.T) {
	Preview = true
	runDatabaseTest(t, func(t *testing.T, srcdb *sql.DB, destdb *sql.DB) {
		setupObjectsTable(srcdb)

		redirectLogs(t, func() {
			expectLogMessage(t, "(not executed)", func() {
				extractLoadDatabase("testsrc", "testdest", "objects", fullStrategyOpts)
			})

			actual, _ := tableExists("testdest", "testsrc_objects")
			assert.False(t, actual)
		})
	})
	Preview = false
}

func runDatabaseTest(t *testing.T, testfn func(*testing.T, *sql.DB, *sql.DB)) {
	Databases["testsrc"] = Database{"sqlite://:memory:", map[string]string{}, true}
	dbSrc, err := connectDatabase("testsrc")
	if err != nil {
		assert.FailNow(t, "%w", err)
	}
	defer delete(dbs, "testsrc")

	Databases["testdest"] = Database{"sqlite://:memory:", map[string]string{}, false}
	dbDest, err := connectDatabase("testdest")
	if err != nil {
		assert.FailNow(t, "%w", err)
	}
	defer delete(dbs, "testdest")

	testfn(t, dbSrc, dbDest)
}

func assertRowCount(t *testing.T, expected int, database *sql.DB, table string) {
	row := database.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	var count int
	err := row.Scan(&count)
	if err != nil {
		assert.FailNow(t, "%w", err)
	}
	assert.Equal(t, expected, count, "the number of rows is different than expected")
}

func assertCsvCellContents(t *testing.T, expected string, csvfilename string, row int, col int) {
	csvfile, err := os.Open(csvfilename)
	if err != nil {
		assert.FailNow(t, "%w", err)
	}

	reader := csv.NewReader(bufio.NewReader(csvfile))

	rowItr := 0

	for {
		line, err := reader.Read()
		if err == io.EOF {
			assert.FailNow(t, "fewer than %d rows in CSV", row)
		} else if err != nil {
			assert.FailNow(t, "%w", err)
		}

		if row != rowItr {
			rowItr++
			break
		}

		assert.EqualValues(t, expected, line[col])
		return
	}
}

func setupObjectsTable(db *sql.DB) {
	objects := schema.Table{"", "objects", make([]schema.Column, 3)}
	objects.Columns[0] = schema.Column{"id", schema.INTEGER, map[schema.Option]int{schema.BYTES: 8}}
	objects.Columns[1] = schema.Column{"name", schema.STRING, map[schema.Option]int{schema.LENGTH: 255}}
	objects.Columns[2] = schema.Column{"updated_at", schema.TIMESTAMP, map[schema.Option]int{}}

	db.Exec(objects.GenerateCreateTableStatement("objects"))
	statement, _ := db.Prepare("INSERT INTO objects (id, name, updated_at) VALUES (?, ?, ?)")
	statement.Exec(1, "book", time.Now().Add(-7*24*time.Hour))
	statement.Exec(2, "tv", time.Now().Add(-1*24*time.Hour))
	statement.Exec(3, "chair", time.Now())
	statement.Close()
}
