package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	widgetsTableDefinition = readTableFromConfigFile("test/example_widgets.yaml")
)

func TestLoadNewTable(t *testing.T) {
	Connections["test1"] = Connection{"test1", Configuration{"sqlite://:memory:"}}
	db1, _ := connectDatabase("test1")

	Connections["test2"] = Connection{"test2", Configuration{"sqlite://:memory:"}}
	db2, _ := connectDatabase("test2")

	db1.Exec(widgetsTableDefinition.generateCreateTableStatement("widgets"))
	importCSV("test1", "widgets", "test/example_widgets.csv")

	load("test1", "test2", "widgets")

	assertRowCount(t, 3, db2, "test1_widgets")

	db1.Exec("DROP TABLE widgets;")
	db2.Exec("DROP TABLE test1_widgets;")
}

func TestLoadSourceHasAdditionalColumn(t *testing.T) {
	Connections["test1"] = Connection{"test1", Configuration{"sqlite://:memory:"}}
	db1, _ := connectDatabase("test1")

	Connections["test2"] = Connection{"test2", Configuration{"sqlite://:memory:"}}
	db2, _ := connectDatabase("test2")

	// Create a new Table Definition, same as widgets, but without the `description` column
	widgetsWithoutDescription := Table{"example", "widgets", make([]Column, 0)}
	widgetsWithoutDescription.Columns = append(widgetsWithoutDescription.Columns, widgetsTableDefinition.Columns[:2]...)
	widgetsWithoutDescription.Columns = append(widgetsWithoutDescription.Columns, widgetsTableDefinition.Columns[3:]...)

	db1.Exec(widgetsTableDefinition.generateCreateTableStatement("widgets"))
	db2.Exec(widgetsWithoutDescription.generateCreateTableStatement("test1_widgets"))
	importCSV("test1", "widgets", "test/example_widgets.csv")

	var logBuffer bytes.Buffer
	log.SetOutput(&logBuffer)

	load("test1", "test2", "widgets")

	assertRowCount(t, 3, db2, "test1_widgets")
	assert.Contains(t, logBuffer.String(), "source table column `description` excluded")

	log.SetOutput(os.Stderr)
	db1.Exec("DROP TABLE widgets;")
	db2.Exec("DROP TABLE test1_widgets;")
}

func TestLoadStringNotLongEnough(t *testing.T) {
	Connections["test1"] = Connection{"test1", Configuration{"sqlite://:memory:"}}
	db1, _ := connectDatabase("test1")

	Connections["test2"] = Connection{"test2", Configuration{"sqlite://:memory:"}}
	db2, _ := connectDatabase("test2")

	// Create a new Table Definition, same as widgets, but with name LENGTH changed to 32
	widgetsWithShortName := Table{"example", "widgets", make([]Column, len(widgetsTableDefinition.Columns))}
	copy(widgetsWithShortName.Columns, widgetsTableDefinition.Columns)
	widgetsWithShortName.Columns[1] = Column{"name", STRING, map[Option]int{LENGTH: 32}}

	db1.Exec(widgetsTableDefinition.generateCreateTableStatement("widgets"))
	db2.Exec(widgetsWithShortName.generateCreateTableStatement("test1_widgets"))

	var logBuffer bytes.Buffer
	log.SetOutput(&logBuffer)

	load("test1", "test2", "widgets")

	assert.Contains(t, logBuffer.String(), "For string column `name`, destination LENGTH is too short")

	log.SetOutput(os.Stderr)
	db1.Exec("DROP TABLE widgets;")
	db2.Exec("DROP TABLE test1_widgets;")
}

func assertRowCount(t *testing.T, expected int, database *sql.DB, table string) {
	row := database.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	var count int
	err := row.Scan(&count)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, expected, count, "the number of rows is different than expected")
}
