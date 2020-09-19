package schema

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestSQLiteInspection(t *testing.T) {
	withDatabase(t, "sqlite://:memory:", func(db Database) {
		testColumnCases(t, db, genericCases)

		testColumnCases(t, db, genericStringCases)
	})
}

func TestWarnAndIgnoreUnsupportedColumns(t *testing.T) {
	withDatabase(t, "sqlite://:memory:", func(db Database) {
		testSkippedColumnCases(t, db, []string{"BINARY"})
	})
}

func TestSQLiteTableGeneration(t *testing.T) {
	withDatabase(t, "sqlite://:memory:", func(db Database) {
		testTableGeneration(t, db)
	})
}
