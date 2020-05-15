package main

import (
	"database/sql"
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
	case "help", "-h", "--help":
		help()
	case "about-db":
		aboutDB(opts.Source)
	case "db-terminal":
		dbTerminal(opts.Source)
	case "extract":
		extract(opts.FromSource, opts.TableName)
	case "extract-load":
		strategyOpts := make(map[string]string)
		switch opts.Strategy {
		case "full":
			// None
		case "incremental":
			strategyOpts["primary_key"] = opts.PrimaryKey
			strategyOpts["modified_at_column"] = opts.ModifiedAtColumn
			strategyOpts["hours_ago"] = opts.HoursAgo
		default:
			log.Fatal("Invalid strategy, acceptable options: full, incremental")
		}
		load(opts.FromSource, opts.ToSource, opts.TableName, opts.Strategy, strategyOpts)
	case "import-csv":
		importCSV(opts.Source, opts.TableName, opts.File)
	case "list-tables":
		listTables(opts.Source)
	case "drop-table":
		dropTable(opts.Source, opts.TableName)
	case "create-destination-table":
		createDestinationTable(opts.FromSource, opts.ToSource, opts.TableName)
	case "create-destination-table-from-config-file":
		createDestinationTableFromConfigFile(opts.Source, opts.File)
	case "describe-table":
		describeTable(opts.Source, opts.TableName)
	case "table-metadata":
		tableMetadata(opts.Source, opts.TableName)
	case "transform":
		updateTransform(opts.Source, opts.TableName)
	}
}

func extract(source string, table string) {
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
	url := Connections[source].Config.URL
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
