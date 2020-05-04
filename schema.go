package main

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/jimsmart/schema"
)

type Table struct {
	Source  string
	Table   string
	Columns []Column
}

// Column is our representation of a column within a table
type Column struct {
	Name     string
	DataType DataType
	Options  map[Option]int
}

type DataType int

const (
	INTEGER DataType = iota
	DECIMAL
	FLOAT
	STRING
	DATE
	TIMESTAMP
	BOOLEAN
)

type Option int

const (
	LENGTH Option = iota
	PRECISION
	SCALE
	BYTES
)

// Supported Data Types:
// * INT (Number of Bytes, <8)
// * DECIMAL (Precision)
// * FLOAT (4 or 8 bytes)
// * STRING (Length)
// * Date
// * Timestamp
// * Boolean
// Future: * BLOB

func dumpTableMetadata(source string, tableName string) (*Table, error) {
	database, err := connectDatabase(source)
	if err != nil {
		return nil, err
	}

	table := Table{source, tableName, nil}
	columnTypes, err := schema.Table(database, tableName)
	if err != nil {
		return nil, err
	}

	for _, columnType := range columnTypes {
		column := Column{}
		column.Name = columnType.Name()
		column.DataType, err = determineDataType(columnType)
		column.Options, err = determineOptions(columnType, column.DataType)
		if err != nil {
			log.Fatal(err)
		}
		table.Columns = append(table.Columns, column)
	}

	return &table, nil
}

func determineDataType(columnType *sql.ColumnType) (DataType, error) {
	databaseTypeName := strings.ToLower(columnType.DatabaseTypeName())
	if strings.Contains(databaseTypeName, "varchar") {
		return STRING, nil
	} else if strings.HasPrefix(databaseTypeName, "int") {
		return INTEGER, nil
	} else if strings.HasPrefix(databaseTypeName, "decimal") {
		return DECIMAL, nil
	} else if strings.HasPrefix(databaseTypeName, "numeric") {
		return DECIMAL, nil
	} else if strings.HasPrefix(databaseTypeName, "bool") {
		return BOOLEAN, nil
	} else if strings.HasPrefix(databaseTypeName, "datetime") {
		return TIMESTAMP, nil
	} else if strings.HasPrefix(databaseTypeName, "timestamp") {
		return TIMESTAMP, nil
	} else if databaseTypeName == "date" {
		return DATE, nil
	}

	return -1, fmt.Errorf("unable to determine data type for: %s (%s)", columnType.Name(), databaseTypeName)
}

func determineOptions(columnType *sql.ColumnType, dataType DataType) (map[Option]int, error) {
	options := make(map[Option]int)
	optionsRegex, _ := regexp.Compile("\\((\\d+)(,(\\d+))?\\)$")
	switch dataType {
	case INTEGER:
		options[BYTES] = 8
	case STRING:
		length, ok := columnType.Length()
		if ok {
			options[LENGTH] = int(length)
		} else if optionsRegex.MatchString(columnType.DatabaseTypeName()) {
			length, err := strconv.Atoi(optionsRegex.FindStringSubmatch(columnType.DatabaseTypeName())[1])
			if err != nil {
				return nil, err
			}
			options[LENGTH] = length
		} else {
			return nil, fmt.Errorf("unable to determine options for: %s (%s)", columnType.Name(), columnType.DatabaseTypeName())
		}
	case DECIMAL:
		precision, scale, ok := columnType.DecimalSize()
		if ok {
			options[PRECISION] = int(precision)
			options[SCALE] = int(scale)
		} else if optionsRegex.MatchString(columnType.DatabaseTypeName()) {
			precision, err := strconv.Atoi(optionsRegex.FindStringSubmatch(columnType.DatabaseTypeName())[1])
			if err != nil {
				return nil, err
			}

			scale, err := strconv.Atoi(optionsRegex.FindStringSubmatch(columnType.DatabaseTypeName())[3])
			if err != nil {
				return nil, err
			}
			options[PRECISION] = precision
			options[SCALE] = scale
		} else {
			return nil, fmt.Errorf("unable to determine options for: %s (%s)", columnType.Name(), columnType.DatabaseTypeName())
		}
	}

	return options, nil
}
