package schema

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestSQLiteInspection(t *testing.T) {
	withDb(t, "sqlite://:memory:", func(db *sql.DB) {
		testColumnCases(t, db, genericCases)

		testColumnCases(t, db, genericStringCases)
	})
}

func TestWarnAndIgnoreUnsupportedColumns(t *testing.T) {
	withDb(t, "sqlite://:memory:", func(db *sql.DB) {
		testSkippedColumnCases(t, db, []string{"BINARY"})
	})
}

func TestSQLiteTableGeneration(t *testing.T) {
	withDb(t, "sqlite://:memory:", func(db *sql.DB) {
		testTableGeneration(t, db)
	})
}

func TestSQLiteAddColumn(t *testing.T) {
	withDb(t, "sqlite://:memory:", func(db *sql.DB) {
		testAddColumn(t, db)
	})
}
