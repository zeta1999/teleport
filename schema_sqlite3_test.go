package main

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setup() {
	Databases["test"] = Database{"sqlite://:memory:", map[string]string{}, false}
	db, _ := connectDatabase("test")

	db.Exec(`
		CREATE TABLE IF NOT EXISTS widgets (
			id INTEGER NOT NULL,
			name VARCHAR(255),
			description VARCHAR(65536),
			price DECIMAL(10,2),
			quantity INTEGER,
			active BOOLEAN,
			launch_date DATE,
			updated_at DATETIME NOT NULL,
			created_at DATETIME NOT NULL
		)
	`)
}

func TestDumpTableMetadataSQLite3(t *testing.T) {
	setup()

	table, err := dumpTableMetadata("test", "widgets")
	if err != nil {
		log.Fatal(err)
	}
	assert.Len(t, table.Columns, 9)

	assert.Equal(t, "id", table.Columns[0].Name)
	assert.Equal(t, INTEGER, table.Columns[0].DataType)
	assert.Equal(t, 8, table.Columns[0].Options[BYTES])

	assert.Equal(t, "name", table.Columns[1].Name)
	assert.Equal(t, STRING, table.Columns[1].DataType)
	assert.Equal(t, 255, table.Columns[1].Options[LENGTH])

	assert.Equal(t, "description", table.Columns[2].Name)
	assert.Equal(t, STRING, table.Columns[2].DataType)
	assert.Equal(t, 65536, table.Columns[2].Options[LENGTH])

	assert.Equal(t, "price", table.Columns[3].Name)
	assert.Equal(t, DECIMAL, table.Columns[3].DataType)
	assert.Equal(t, 10, table.Columns[3].Options[PRECISION])
	assert.Equal(t, 2, table.Columns[3].Options[SCALE])

	assert.Equal(t, "quantity", table.Columns[4].Name)
	assert.Equal(t, INTEGER, table.Columns[4].DataType)
	assert.Equal(t, 8, table.Columns[4].Options[BYTES])

	assert.Equal(t, "active", table.Columns[5].Name)
	assert.Equal(t, BOOLEAN, table.Columns[5].DataType)

	assert.Equal(t, "launch_date", table.Columns[6].Name)
	assert.Equal(t, DATE, table.Columns[6].DataType)

	assert.Equal(t, "updated_at", table.Columns[7].Name)
	assert.Equal(t, TIMESTAMP, table.Columns[7].DataType)

	assert.Equal(t, "created_at", table.Columns[8].Name)
	assert.Equal(t, TIMESTAMP, table.Columns[8].DataType)
}
