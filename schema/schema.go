package schema

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	xschema "github.com/jimsmart/schema"
)

// Table is our representation of a Table in a relational database
type Table struct {
	Source  string   `yaml:"source"`
	Name    string   `yaml:"table"`
	Columns []Column `yaml:"columns"`
}

// Column is our representation of a column within a table
type Column struct {
	Name     string         `yaml:"name"`
	DataType DataType       `yaml:"datatype"`
	Options  map[Option]int `yaml:"options"`
}

type DataType string

const (
	// Numeric types
	INTEGER DataType = "integer"
	DECIMAL DataType = "decimal"
	FLOAT   DataType = "float"

	// String types
	STRING DataType = "string"
	TEXT   DataType = "text"

	// Time types
	DATE      DataType = "date"
	TIMESTAMP DataType = "timestamp"

	// Other types
	BOOLEAN DataType = "boolean"
)

type Option string

const (
	LENGTH    Option = "length"
	PRECISION Option = "precision"
	SCALE     Option = "scale"
	BYTES     Option = "bytes"
)

// MaxLength represents the maximum possible length for the data type
const MaxLength int = -1

// Supported Data Types:
// * INT (Number of Bytes, <8)
// * DECIMAL (Precision)
// * FLOAT (4 or 8 bytes)
// * STRING (Length)
// * Date
// * Timestamp
// * Boolean
// Future: * BLOB

func DumpTableMetadata(database *sql.DB, tableName string) (*Table, error) {
	table := Table{"", tableName, nil}
	columnTypes, err := xschema.Table(database, tableName)
	if err != nil {
		return nil, err
	}

	for _, columnType := range columnTypes {
		column := Column{}
		column.Name = columnType.Name()
		column.DataType, err = determineDataType(columnType)
		if err != nil {
			return nil, err
		}
		column.Options, err = determineOptions(columnType, column.DataType)
		if err != nil {
			return nil, err
		}
		table.Columns = append(table.Columns, column)
	}

	return &table, nil
}

func determineDataType(columnType *sql.ColumnType) (DataType, error) {
	databaseTypeName := strings.ToLower(columnType.DatabaseTypeName())
	if strings.Contains(databaseTypeName, "varchar") {
		return STRING, nil
	} else if strings.Contains(databaseTypeName, "text") {
		return TEXT, nil
	} else if strings.Contains(databaseTypeName, "tinyint") {
		return BOOLEAN, nil
	} else if strings.HasPrefix(databaseTypeName, "int") {
		return INTEGER, nil
	} else if strings.HasSuffix(databaseTypeName, "int") {
		return INTEGER, nil
	} else if strings.HasPrefix(databaseTypeName, "decimal") {
		return DECIMAL, nil
	} else if strings.HasPrefix(databaseTypeName, "numeric") {
		return DECIMAL, nil
	} else if strings.Contains(databaseTypeName, "num") {
		return FLOAT, nil
	} else if strings.HasPrefix(databaseTypeName, "bool") {
		return BOOLEAN, nil
	} else if strings.HasPrefix(databaseTypeName, "datetime") {
		return TIMESTAMP, nil
	} else if strings.HasPrefix(databaseTypeName, "timestamp") {
		return TIMESTAMP, nil
	} else if databaseTypeName == "date" {
		return DATE, nil
	}

	return "", fmt.Errorf("unable to determine data type for: %s (%s)", columnType.Name(), databaseTypeName)
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
			options[LENGTH] = MaxLength
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

func (table *Table) GenerateCreateTableStatement(name string) string {
	statement := fmt.Sprintf("CREATE TABLE %s (\n", name)
	for _, column := range table.Columns {
		statement += fmt.Sprintf("%s %s,\n", column.Name, column.generateDataTypeExpression())
	}
	statement = strings.TrimSuffix(statement, ",\n")
	statement += "\n);"

	return statement
}

func (column *Column) generateDataTypeExpression() string {
	switch column.DataType {
	case INTEGER:
		bytes := column.Options[BYTES]

		return fmt.Sprintf("INT%d", bytes)
	case STRING:
		length := column.Options[LENGTH]

		// For columns with LENGTH = max, use 8192 characters for now
		if length == MaxLength {
			length = 8192
		}

		return fmt.Sprintf("VARCHAR(%d)", length)
	case DECIMAL:
		precision := column.Options[PRECISION]
		scale := column.Options[SCALE]

		return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
	}

	return strings.ToUpper(string(column.DataType))
}

func (table *Table) ContainsColumnWithSameName(c Column) bool {
	for _, column := range table.Columns {
		if c.Name == column.Name {
			return true
		}
	}
	return false
}

func (table *Table) NotContainsColumnWithSameName(c Column) bool {
	for _, column := range table.Columns {
		if c.Name == column.Name {
			return false
		}
	}
	return true
}
