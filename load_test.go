package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	widgetsTableDefinition = readTableFromConfigFile("test/example_widgets.yaml")
	fullStrategyOpts       = map[string]string{}
)

func TestLoadNewTable(t *testing.T) {
	Connections["test1"] = Connection{"test1", Configuration{"sqlite://:memory:", map[string]string{}}}
	db1, _ := connectDatabase("test1")

	Connections["test2"] = Connection{"test2", Configuration{"sqlite://:memory:", map[string]string{}}}
	db2, _ := connectDatabase("test2")

	db1.Exec(widgetsTableDefinition.generateCreateTableStatement("widgets"))
	importCSV("test1", "widgets", "test/example_widgets.csv", widgetsTableDefinition.Columns)

	load("test1", "test2", "widgets", "full", fullStrategyOpts)

	assertRowCount(t, 3, db2, "test1_widgets")

	db1.Exec("DROP TABLE widgets;")
	db2.Exec("DROP TABLE test1_widgets;")
}

func TestLoadSourceHasAdditionalColumn(t *testing.T) {
	Connections["test1"] = Connection{"test1", Configuration{"sqlite://:memory:", map[string]string{}}}
	db1, _ := connectDatabase("test1")

	Connections["test2"] = Connection{"test2", Configuration{"sqlite://:memory:", map[string]string{}}}
	db2, _ := connectDatabase("test2")

	// Create a new Table Definition, same as widgets, but without the `description` column
	widgetsWithoutDescription := Table{"example", "widgets", make([]Column, 0)}
	widgetsWithoutDescription.Columns = append(widgetsWithoutDescription.Columns, widgetsTableDefinition.Columns[:2]...)
	widgetsWithoutDescription.Columns = append(widgetsWithoutDescription.Columns, widgetsTableDefinition.Columns[3:]...)

	db1.Exec(widgetsTableDefinition.generateCreateTableStatement("widgets"))
	db2.Exec(widgetsWithoutDescription.generateCreateTableStatement("test1_widgets"))
	importCSV("test1", "widgets", "test/example_widgets.csv", widgetsTableDefinition.Columns)

	var logBuffer bytes.Buffer
	log.SetOutput(&logBuffer)

	load("test1", "test2", "widgets", "full", fullStrategyOpts)

	log.SetOutput(os.Stdout)

	assertRowCount(t, 3, db2, "test1_widgets")
	assert.Contains(t, logBuffer.String(), "source table column `description` excluded")

	db1.Exec("DROP TABLE widgets;")
	db2.Exec("DROP TABLE test1_widgets;")
}

func TestLoadStringNotLongEnough(t *testing.T) {
	Connections["test1"] = Connection{"test1", Configuration{"sqlite://:memory:", map[string]string{}}}
	db1, _ := connectDatabase("test1")

	Connections["test2"] = Connection{"test2", Configuration{"sqlite://:memory:", map[string]string{}}}
	db2, _ := connectDatabase("test2")

	// Create a new Table Definition, same as widgets, but with name LENGTH changed to 32
	widgetsWithShortName := Table{"example", "widgets", make([]Column, len(widgetsTableDefinition.Columns))}
	copy(widgetsWithShortName.Columns, widgetsTableDefinition.Columns)
	widgetsWithShortName.Columns[1] = Column{"name", STRING, map[Option]int{LENGTH: 32}}

	db1.Exec(widgetsTableDefinition.generateCreateTableStatement("widgets"))
	db2.Exec(widgetsWithShortName.generateCreateTableStatement("test1_widgets"))

	var logBuffer bytes.Buffer
	log.SetOutput(&logBuffer)

	load("test1", "test2", "widgets", "full", fullStrategyOpts)

	log.SetOutput(os.Stdout)

	assert.Contains(t, logBuffer.String(), "For string column `name`, destination LENGTH is too short")

	db1.Exec("DROP TABLE widgets;")
	db2.Exec("DROP TABLE test1_widgets;")
}

func TestIncrementalLoad(t *testing.T) {
	Connections["test1"] = Connection{"test1", Configuration{"sqlite://:memory:", map[string]string{}}}
	db1, _ := connectDatabase("test1")

	Connections["test2"] = Connection{"test2", Configuration{"sqlite://:memory:", map[string]string{}}}
	db2, _ := connectDatabase("test2")

	objects := Table{"example", "objects", make([]Column, 3)}
	objects.Columns[0] = Column{"id", INTEGER, map[Option]int{BYTES: 8}}
	objects.Columns[1] = Column{"name", STRING, map[Option]int{LENGTH: 255}}
	objects.Columns[2] = Column{"updated_at", TIMESTAMP, map[Option]int{}}

	db1.Exec(objects.generateCreateTableStatement("objects"))
	statement, _ := db1.Prepare("INSERT INTO objects (id, name, updated_at) VALUES (?, ?, ?)")
	statement.Exec(1, "book", time.Now().Add(-7*24*time.Hour))
	statement.Exec(2, "tv", time.Now().Add(-1*24*time.Hour))
	statement.Exec(3, "chair", time.Now())
	statement.Close()

	strategyOpts := make(map[string]string)
	strategyOpts["primary_key"] = "id"
	strategyOpts["modified_at_column"] = "updated_at"
	strategyOpts["hours_ago"] = "36"
	load("test1", "test2", "objects", "incremental", strategyOpts)

	assertRowCount(t, 2, db2, "test1_objects")

	db1.Exec("DROP TABLE objects;")
	db2.Exec("DROP TABLE test1_objects;")
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
