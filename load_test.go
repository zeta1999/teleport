package main

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	widgetsTableDefinition = readTableFromConfigFile("test/example_widgets.yaml")
)

func TestLoad(t *testing.T) {
	Connections["test1"] = Connection{"test1", Configuration{"sqlite://:memory:"}}
	db1, _ := connectDatabase("test1")

	Connections["test2"] = Connection{"test2", Configuration{"sqlite://:memory:"}}
	db2, _ := connectDatabase("test2")

	db1.Exec(widgetsTableDefinition.generateCreateTableStatement("widgets"))
	importCSV("test1", "widgets", "test/example_widgets.csv")

	load("test1", "test2", "widgets")

	assertRowCount(t, 3, db2, "test1_widgets")
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
