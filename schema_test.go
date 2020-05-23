package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateCreateTableStatement(t *testing.T) {
	table := widgetsTable()
	expected := squish(`CREATE TABLE source_widgets (
		id INT8,
		name VARCHAR(255),
		active BOOLEAN,
		price DECIMAL(10,2)
	);`)
	assert.Equal(t, expected, squish(table.generateCreateTableStatement("source_widgets")))

}

func TestTableExists(t *testing.T) {
	Connections["test"] = Connection{"test", Configuration{"sqlite://:memory:", map[string]string{}}}
	db, _ := connectDatabase("test")

	db.Exec("CREATE TABLE IF NOT EXISTS animals (id integer, name varchar(255))")

	actual, _ := tableExists("test", "does_not_exist")
	assert.False(t, actual)

	actual, _ = tableExists("test", "animals")
	assert.True(t, actual)
}

func TestCreateTable(t *testing.T) {
	Connections["test"] = Connection{"test", Configuration{"sqlite://:memory:", map[string]string{}}}
	db, _ := connectDatabase("test")

	table := widgetsTable()

	assert.NoError(t, createTable(db, "newtable", &table))
	actual, _ := tableExists("test", "animals")
	assert.True(t, actual)
}

func widgetsTable() Table {
	columns := make([]Column, 0)
	columns = append(columns, Column{"id", INTEGER, map[Option]int{BYTES: 8}})
	columns = append(columns, Column{"name", STRING, map[Option]int{LENGTH: 255}})
	columns = append(columns, Column{"active", BOOLEAN, map[Option]int{}})
	columns = append(columns, Column{"price", DECIMAL, map[Option]int{PRECISION: 10, SCALE: 2}})

	return Table{"source", "widgets", columns}
}
