package main

import (
	"log"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	opts := parseArguments()
	readConnections()
	readEndpoints()

	switch opts.Command {
	case "help", "-h", "--help":
		help()
	case "about-db":
		aboutDB(opts.Source)
	case "db-terminal":
		dbTerminal(opts.Source)
	case "extract":
		extractDB(opts.FromSource, opts.TableName)
	case "extract-api":
		extractAPI(opts.FromSource)
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
	case "extract-load-api":
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
		loadAPI(opts.FromSource, opts.ToSource, opts.TableName, opts.Strategy, strategyOpts)
	case "import-csv":
		tableDefinition, err := dumpTableMetadata(opts.Source, opts.TableName)
		if err != nil {
			log.Fatal("Dump Table Metadata Error:", err)
		}
		importCSV(opts.Source, opts.TableName, opts.File, tableDefinition.Columns)
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
