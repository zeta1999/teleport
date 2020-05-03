package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/jimsmart/schema"
)

// Table is our representation of a table
type Table struct {
	Source  string
	Table   string
	Columns []Column
}

// Column is our representation of a column within a table
type Column struct {
	Name     string
	Limit    int
	ByteSize int
}

// Supported Data Types:
// * INT (Number of Bytes, <8)
// * DECIMAL (Precision)
// * FLOAT (4 or 8 bytes)
// * STRING (Length)
// * Date
// * DateTime
// * Boolean
// * BLOB

func describeTable(source string, tableName string) {
	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	table := Table{source, tableName, nil}

	columnTypes, err := schema.Table(database, tableName)
	if err != nil {
		log.Fatal("Describe Table Error:", err)
	}

	columns, err := translateToTableDescriptionSQLite3(columnTypes)
	if err != nil {
		log.Fatal("Column Type Description Error:", err)
	}

	table.Columns = columns
	fmt.Println(table)
}

func translateToTableDescriptionSQLite3(columnTypes []*sql.ColumnType) ([]Column, error) {
	// TODO
	for _, columnType := range columnTypes {
		fmt.Println("_COLUMN_")
		fmt.Println(columnType.Name())
		fmt.Println(columnType.DatabaseTypeName())
		fmt.Println(columnType.DecimalSize())
		fmt.Println(columnType.Length())
		fmt.Println(columnType.Nullable())
		fmt.Println(columnType.ScanType())
	}

	return nil, nil
}
