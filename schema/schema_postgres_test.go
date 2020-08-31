package schema

import (
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
)

func TestPostgresInspection(t *testing.T) {
	withDb(t, "postgres://postgres@localhost:45432/?sslmode=disable", func(db *sql.DB) {
		testColumnCases(t, db, genericCases)

		testColumnCases(t, db, genericStringCases)

		// Serial Types
		testColumnCases(t, db, []struct {
			originalDataTypes  []string
			column             Column
			createTabeDataType string
		}{
			{
				[]string{"SERIAL", "SERIAL4"},
				Column{"", INTEGER, map[Option]int{BYTES: 8}},
				"INT8", // TODO: options support
			},
			{
				[]string{"SERIAL8", "BIGSERIAL"},
				Column{"", INTEGER, map[Option]int{BYTES: 8}},
				"INT8",
			},
			{
				[]string{"SERIAL2", "SMALLSERIAL"},
				Column{"", INTEGER, map[Option]int{BYTES: 8}},
				"INT8", // TODO: options support
			},
		})

		// Special Types
		testColumnCases(t, db, []struct {
			originalDataTypes  []string
			column             Column
			createTabeDataType string
		}{
			{
				[]string{"MONEY"},
				Column{"", DECIMAL, map[Option]int{PRECISION: 16, SCALE: 2}},
				"DECIMAL(16,2)",
			},
			{
				[]string{"inet", "uuid", "macaddr", "cidr"},
				Column{"", STRING, map[Option]int{LENGTH: 255}},
				"VARCHAR(255)",
			},
			{
				[]string{"xml", "json"},
				Column{"", TEXT, map[Option]int{}},
				"TEXT",
			},
		})
	})
}

func TestPostgresTableGeneration(t *testing.T) {
	withDb(t, "postgres://postgres@localhost:45432/?sslmode=disable", func(db *sql.DB) {
		testTableGeneration(t, db)
	})
}

func TestPostgresAddColumn(t *testing.T) {
	withDb(t, "postgres://postgres@localhost:45432/?sslmode=disable", func(db *sql.DB) {
		testAddColumn(t, db)
	})
}
