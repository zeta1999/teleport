package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"github.com/jimsmart/schema"
	"gopkg.in/yaml.v2"
)

type Table struct {
	Source  string   `yaml:"source"`
	Table   string   `yaml:"table"`
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
	INTEGER   DataType = "integer"
	DECIMAL   DataType = "decimal"
	FLOAT     DataType = "float"
	STRING    DataType = "string"
	DATE      DataType = "date"
	TIMESTAMP DataType = "timestamp"
	BOOLEAN   DataType = "boolean"
)

type Option string

const (
	LENGTH    Option = "length"
	PRECISION Option = "precision"
	SCALE     Option = "scale"
	BYTES     Option = "bytes"
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
		if err != nil {
			log.Fatal(err)
		}
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
	} else if strings.HasSuffix(databaseTypeName, "int") {
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
			// fmt.Println(columnType)
			// return nil, fmt.Errorf("unable to determine options for: %s (%s)", columnType.Name(), columnType.DatabaseTypeName())
			options[LENGTH] = 4096
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

func (table *Table) generateCreateTableStatement(name string) string {
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

		return fmt.Sprintf("VARCHAR(%d)", length)
	case DECIMAL:
		precision := column.Options[PRECISION]
		scale := column.Options[SCALE]

		return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
	}

	return strings.ToUpper(string(column.DataType))
}

func tableExists(source string, tableName string) bool {
	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	tables, err := schema.TableNames(database)
	if err != nil {
		log.Fatal(err)
	}

	for _, table := range tables {
		if table == tableName {
			return true
		}
	}

	return false
}

func createTable(database *sql.DB, tableName string, table *Table) error {
	statement := table.generateCreateTableStatement(tableName)

	_, err := database.Exec(statement)

	return err
}

func listTables(source string) {
	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	tables, err := schema.TableNames(database)
	if err != nil {
		log.Fatal("Database Error:", err)
	}
	for _, tablename := range tables {
		fmt.Println(tablename)
	}
}

func dropTable(source string, table string) {
	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	if !tableExists(source, table) {
		log.Fatalf("table \"%s\" not found in \"%s\"", table, source)
	}

	_, err = database.Exec(fmt.Sprintf("DROP TABLE %s", table))
	if err != nil {
		log.Fatal(err)
	}
}

func createDestinationTable(source string, destination string, sourceTableName string) {
	table, err := dumpTableMetadata(source, sourceTableName)
	if err != nil {
		log.Fatal("Table Metadata Error:", err)
	}

	destinationDatabase, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Connect Error:", err)
	}

	err = createTable(destinationDatabase, fmt.Sprintf("%s_%s", source, sourceTableName), table)

	if err != nil {
		log.Fatal(err)
	}
}

func createDestinationTableFromConfigFile(source string, file string) {
	table := readTableFromConfigFile(file)

	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Connect Error:", err)
	}

	statement := table.generateCreateTableStatement(fmt.Sprintf("%s_%s", table.Source, table.Table))

	_, err = database.Exec(statement)
	if err != nil {
		log.Fatal(err)
	}
}

func aboutDB(source string) {
	fmt.Println("Name: ", source)
	fmt.Printf("Type: %s\n", DbDialect(Connections[source]).HumanName)
}

func dbTerminal(source string) {
	command := DbDialect(Connections[source]).TerminalCommand
	if command == "" {
		log.Fatalf("Not implemented for this database type")
	}

	binary, err := exec.LookPath(command)
	if err != nil {
		log.Fatalf("command exec err (%s): %s", command, err)
	}

	env := os.Environ()

	err = syscall.Exec(binary, []string{command, Connections[source].Config.URL}, env)
	if err != nil {
		log.Fatalf("Syscall error: %s", err)
	}

}

func describeTable(source string, tableName string) {
	table, err := dumpTableMetadata(source, tableName)
	if err != nil {
		log.Fatal("Describe Table Error:", err)
	}

	fmt.Println("Source: ", table.Source)
	fmt.Println("Table: ", table.Table)
	fmt.Println()
	fmt.Println("Columns:")
	fmt.Println("========")
	for _, column := range table.Columns {
		fmt.Print(column.Name, " | ", column.DataType)
		if len(column.Options) > 0 {
			fmt.Print(" ( ")
			for option, value := range column.Options {
				fmt.Print(option, ": ", value, ", ")

			}
			fmt.Print(" )")
		}
		fmt.Println()
	}
}

func tableMetadata(source string, tableName string) {
	table, err := dumpTableMetadata(source, tableName)
	if err != nil {
		log.Fatal("Describe Table Error:", err)
	}

	b, err := yaml.Marshal(table)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(b))
}

func readTableFromConfigFile(file string) *Table {
	var table Table

	yamlFile, err := ioutil.ReadFile(file)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}

	err = yaml.Unmarshal(yamlFile, &table)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	return &table
}

func extractDB(source string, table string) {
	tableDefinition, err := dumpTableMetadata(source, table)
	if err != nil {
		log.Fatal("Dump Table Metadata Error:", err)
	}

	tmpfile, err := exportCSV(source, table, tableDefinition.Columns, "")
	if err != nil {
		log.Fatal("Export CSV error:", err)
	}

	log.Printf("Extracted to: %s\n", tmpfile)
}
