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

func TestSQLiteTableGeneration(t *testing.T) {
	withDb(t, "sqlite://:memory:", func(db *sql.DB) {
		testTableGeneration(t, db)
	})
}
