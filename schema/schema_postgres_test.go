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
