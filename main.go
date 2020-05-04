package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/jimsmart/schema"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/xo/dburl"
)

var (
	dbs = make(map[string]*sql.DB)
)

func main() {
	opts := parseArguments()
	readConnections()

	switch opts.Command {
	case "extract":
		extract(opts.DataSource, opts.TableName)
	case "load":
		load(opts.DataSource, opts.TableName, opts.File)
	case "list-tables":
		listTables(opts.DataSource)
	case "drop-table":
		dropTable(opts.DataSource, opts.TableName)
	case "create-destination-table":
		createDestinationTable(opts.DataSource, opts.DestinationDataSource, opts.TableName)
	case "describe-table":
		describeTable(opts.DataSource, opts.TableName)
	}
}

func extract(source string, table string) {
	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	tables, err := schema.TableNames(database)
	if err != nil {
		log.Fatal(err)
	}
	targetTable := ""
	for _, tablename := range tables {
		if tablename == table {
			targetTable = tablename
		}
	}
	if targetTable == "" {
		log.Fatalf("table \"%s\" not found in \"%s\"", table, source)
	}

	tmpfile, err := ioutil.TempFile("/tmp/", fmt.Sprintf("extract-%s-%s", table, source))
	if err != nil {
		log.Fatal(err)
	}

	rows, err := database.Query(fmt.Sprintf("SELECT * FROM %s", targetTable))
	writer := csv.NewWriter(tmpfile)
	columnNames, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}
	//writer.Write(columnNames)

	rawResult := make([]interface{}, len(columnNames))
	writeBuffer := make([]string, len(columnNames))
	for i := range writeBuffer {
		rawResult[i] = &writeBuffer[i]
	}

	for rows.Next() {
		err := rows.Scan(rawResult...)
		if err != nil {
			log.Fatal(err)
		}

		err = writer.Write(writeBuffer)
		if err != nil {
			log.Fatal(err)
		}
	}

	writer.Flush()

	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Extracted to: %s\n", tmpfile.Name())
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

	tables, err := schema.TableNames(database)
	if err != nil {
		log.Fatal(err)
	}

	targetTable := ""
	for _, tablename := range tables {
		if tablename == table {
			targetTable = tablename
		}
	}
	if targetTable == "" {
		log.Fatalf("table \"%s\" not found in \"%s\"", table, source)
	}

	_, err = database.Exec(fmt.Sprintf("DROP TABLE %s", targetTable))
	if err != nil {
		log.Fatal(err)
	}
}

func createDestinationTable(source string, destination string, table string) {
	sourceDatabase, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Connect Error:", err)
	}

	destinationDatabase, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Connect Error:", err)
	}

	sourceCols, err := schema.Table(sourceDatabase, table)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}
	statement := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s_%s", source, table)
	statement += " (\n"
	for _, col := range sourceCols {
		statement += fmt.Sprintf("%s %s,\n", col.Name(), col.DatabaseTypeName())
	}
	statement = strings.TrimSuffix(statement, ",\n")
	statement += "\n);"

	_, err = destinationDatabase.Exec(statement)
	if err != nil {
		log.Fatal(err)
	}
	destinationDatabase.Close()
	sourceDatabase.Close()
}

func connectDatabase(source string) (*sql.DB, error) {
	if dbs[source] != nil {
		return dbs[source], nil
	}
	url := Connections[source].Config.Url
	database, err := dburl.Open(url)
	if err != nil {
		return nil, err
	}

	err = database.Ping()
	if err != nil {
		return nil, err
	}

	dbs[source] = database
	return dbs[source], nil
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
		fmt.Print(column.Name, " | ", column.DataType.String())
		if len(column.Options) > 0 {
			fmt.Print(" ( ")
			for option, value := range column.Options {
				fmt.Print(strings.ToLower(option.String()), ": ", value, ", ")

			}
			fmt.Print(" )")
		}
		fmt.Println()
	}
}
