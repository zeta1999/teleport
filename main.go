package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jimsmart/schema"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/xo/dburl"
	"gopkg.in/yaml.v2"
)

var (
	dbs = make(map[string]*sql.DB)
)

func main() {
	opts := parseArguments()
	readConnections()

	switch opts.Command {
	case "about-db":
		aboutDB(opts.DataSource)
	case "extract":
		extract(opts.DataSource, opts.TableName)
	case "load":
		load(opts.DataSource, opts.DestinationDataSource, opts.TableName)
	case "import-csv":
		importCSV(opts.DataSource, opts.TableName, opts.File)
	case "list-tables":
		listTables(opts.DataSource)
	case "drop-table":
		dropTable(opts.DataSource, opts.TableName)
	case "create-destination-table":
		createDestinationTable(opts.DataSource, opts.DestinationDataSource, opts.TableName)
	case "create-destination-table-from-config-file":
		createDestinationTableFromConfigFile(opts.DataSource, opts.File)
	case "describe-table":
		describeTable(opts.DataSource, opts.TableName)
	case "table-metadata":
		tableMetadata(opts.DataSource, opts.TableName)
	case "update-transform":
		updateTransform(opts.DataSource, opts.TableName)
	}
}

func extract(source string, table string) {
	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	if !tableExists(source, table) {
		log.Fatalf("table \"%s\" not found in \"%s\"", table, source)
	}

	tmpfile, err := ioutil.TempFile("/tmp/", fmt.Sprintf("extract-%s-%s", table, source))
	if err != nil {
		log.Fatal(err)
	}

	rows, err := database.Query(fmt.Sprintf("SELECT * FROM %s", table))
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

func updateTransform(source string, transformName string) {
	contents, err := ioutil.ReadFile(strings.Join([]string{"transforms/", transformName, ".sql"}, ""))
	if err != nil {
		log.Fatal(err)
	}

	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Connect Error:", err)
	}

	_, err = database.Exec(fmt.Sprintf(`
		DROP TABLE IF EXISTS staging_%s;

		CREATE TABLE staging_%s AS %s;

		BEGIN;
			DROP TABLE IF EXISTS %s;
			ALTER TABLE staging_%s RENAME TO %s;
		END;
	`, transformName, transformName, contents, transformName, transformName, transformName))
	if err != nil {
		log.Fatal("Transform Error:", err)
	}
}
