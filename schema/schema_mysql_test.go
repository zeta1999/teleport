package schema

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestMySQLInspection(t *testing.T) {
	withDb(t, "mysql://mysql_test_user:password@localhost:43306/test_db", func(db *sql.DB) {
		testColumnCases(t, db, genericCases)

		// String Types - MySQL go db/sql adapter does not support "Length"
		testColumnCases(t, db, []struct {
			originalDataTypes  []string
			column             Column
			createTabeDataType string
		}{
			{
				[]string{"VARCHAR(511)", "CHARACTER VARYING(511)"},
				Column{"", STRING, map[Option]int{LENGTH: -1}}, // TODO: MySQL String length
				"VARCHAR(16380)",
			},
			{
				[]string{"CHAR(127)"},
				Column{"", STRING, map[Option]int{LENGTH: -1}}, // TODO: MySQL String length
				"VARCHAR(16380)",
			},
		})

		// Special Types
		testColumnCases(t, db, []struct {
			originalDataTypes  []string
			column             Column
			createTabeDataType string
		}{
			{
				[]string{"MEDIUMINT"},
				Column{"", INTEGER, map[Option]int{BYTES: 8}},
				"INT8",
			},
			{
				[]string{"TINYINT"},
				Column{"", BOOLEAN, map[Option]int{}},
				"BOOLEAN",
			},
			{
				[]string{"TIME", "YEAR"},
				Column{"", STRING, map[Option]int{LENGTH: 32}},
				"VARCHAR(32)",
			},
		})
	})
}
