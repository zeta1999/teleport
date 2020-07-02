package schema

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xo/dburl"
)

func TestGenerateCreateTableStatement(t *testing.T) {
	table := widgetsTable()
	expected := squish(`CREATE TABLE source_widgets (
		id INT8,
		name VARCHAR(255),
		active BOOLEAN,
		price DECIMAL(10,2)
	);`)
	assert.Equal(t, expected, squish(table.GenerateCreateTableStatement("source_widgets")))

}

func widgetsTable() Table {
	columns := make([]Column, 0)
	columns = append(columns, Column{"id", INTEGER, map[Option]int{BYTES: 8}})
	columns = append(columns, Column{"name", STRING, map[Option]int{LENGTH: 255}})
	columns = append(columns, Column{"active", BOOLEAN, map[Option]int{}})
	columns = append(columns, Column{"price", DECIMAL, map[Option]int{PRECISION: 10, SCALE: 2}})

	return Table{"source", "widgets", columns}
}

func withDb(t *testing.T, connectionString string, testfn func(*sql.DB)) {
	db, err := dburl.Open(connectionString)
	if err != nil {
		assert.FailNow(t, err.Error())
	}

	testfn(db)
}

func testColumnCases(t *testing.T, db *sql.DB, cases []struct {
	originalDataTypes  []string
	column             Column
	createTabeDataType string
}) {

	db.Exec("DROP TABLE IF EXISTS test_table")
	createTableStatement := `
	CREATE TABLE test_table (
		%s
	);
`
	columns := ""
	for cidx, cse := range cases {
		for didx, dataType := range cse.originalDataTypes {
			columns += fmt.Sprintf("col%d%d %s,\n", cidx, didx, dataType)
		}
	}
	columns = strings.TrimSuffix(columns, ",\n")

	_, err := db.Exec(fmt.Sprintf(createTableStatement, columns))
	if err != nil {
		assert.FailNow(t, err.Error(), fmt.Sprintf(createTableStatement, columns))
	}
	defer db.Exec("DROP TABLE test_table")

	table, err := DumpTableMetadata(db, "test_table")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	generatedCreateTableStatement := table.GenerateCreateTableStatement("test_table")

	for cidx, cse := range cases {
		for didx, dataType := range cse.originalDataTypes {
			var col Column
			for _, column := range table.Columns {
				if column.Name == fmt.Sprintf("col%d%d", cidx, didx) {
					col = column
					break
				}
			}
			assert.Equal(t, cse.column.DataType, col.DataType, "DataType: %s, Case: %v", dataType, cse)
			assert.Equal(t, cse.column.Options, col.Options, "DataType: %s, Case: %v", dataType, cse)

			assert.Contains(t, generatedCreateTableStatement, fmt.Sprintf("col%d%d %s", cidx, didx, cse.createTabeDataType), "DataType: %s, Case: %v", dataType, cse)
		}

	}
}

var genericCases = []struct {
	originalDataTypes  []string
	column             Column
	createTabeDataType string
}{
	// Integer Types
	{
		[]string{"INT", "INTEGER", "INT4"},
		Column{"", INTEGER, map[Option]int{BYTES: 8}},
		"INT8", // TODO: options support
	},
	{
		[]string{"INT8", "BIGINT"},
		Column{"", INTEGER, map[Option]int{BYTES: 8}},
		"INT8",
	},
	{
		[]string{"INT2", "SMALLINT"},
		Column{"", INTEGER, map[Option]int{BYTES: 8}},
		"INT8", // TODO: options support
	},

	// Decimal Types
	{
		[]string{"DECIMAL(9,4)", "NUMERIC(9,4)"},
		Column{"", DECIMAL, map[Option]int{PRECISION: 9, SCALE: 4}},
		"DECIMAL(9,4)", // TODO: options support
	},

	// Float Types
	{
		[]string{"REAL", "FLOAT4", "FLOAT"},
		Column{"", FLOAT, map[Option]int{BYTES: 8}},
		"FLOAT8", // TODO: options support
	},
	{
		[]string{"DOUBLE PRECISION", "FLOAT8"},
		Column{"", FLOAT, map[Option]int{BYTES: 8}},
		"FLOAT8", // TODO: options support
	},

	// Boolean Types
	{
		[]string{"BOOLEAN", "BOOL"},
		Column{"", BOOLEAN, map[Option]int{}},
		"BOOLEAN",
	},

	// // Date and Time Types
	{
		[]string{"DATE"},
		Column{"", DATE, map[Option]int{}},
		"DATE",
	},
	{
		[]string{"TIMESTAMP"},
		Column{"", TIMESTAMP, map[Option]int{}},
		"TIMESTAMP",
	},
}

var genericStringCases = []struct {
	originalDataTypes  []string
	column             Column
	createTabeDataType string
}{
	// String Types
	{
		[]string{"VARCHAR(511)", "CHARACTER VARYING(511)"},
		Column{"", STRING, map[Option]int{LENGTH: 511}},
		"VARCHAR(511)",
	},
	{
		[]string{"CHAR(127)"},
		Column{"", STRING, map[Option]int{LENGTH: 127}},
		"VARCHAR(127)",
	},
}
