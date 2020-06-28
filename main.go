package main

import (
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
)

var (
	// Preview indicates preview/dry-run mode is enabled
	Preview bool = false

	// PreviewLimit sets the number of rows to output while in preview mode
	PreviewLimit int = 3
)

func main() {
	if _, ok := os.LookupEnv("PADPATH"); !ok {
		os.Setenv("PADPATH", ".")
	}

	if len(os.Args) == 1 {
		help()
		return
	}

	opts := parseArguments()
	Preview = opts.Preview

	if Preview || opts.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if opts.Command == "new" {
		generateProjectDirectory(os.Args[2])
		return
	}

	readConfiguration()

	switch opts.Command {
	case "version", "-v":
		version()
	case "help", "-h", "--help":
		help()

	// Database Interactions
	case "about-db":
		aboutDB(opts.Source)
	case "db-terminal":
		databaseTerminal(opts.Source)
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
	case "import-csv":
		table, err := dumpTableMetadata(opts.Source, opts.TableName)
		if err != nil {
			log.Fatal(err)
		}
		importCSV(opts.Source, opts.TableName, opts.File, table.Columns)

	// Extract data from a source to csv
	case "extract":
		extractDatabase(opts.FromSource, opts.TableName)
	case "extract-api":
		extractAPI(opts.FromSource)

	// Extract data from a source and load into datawarehouse
	case "extract-load":
		extractLoadDatabase(opts.FromSource, opts.ToSource, opts.TableName, parseStrategyOptions())
	case "extract-load-api":
		extractLoadAPI(opts.FromSource, opts.ToSource)

	// Run Transform within datawarehouse
	case "transform":
		updateTransform(opts.Source, opts.TableName)

	// Handle invalid command
	default:
		fmt.Printf("Error: '%s' is an invalid command\n", os.Args[1])
		listCommands()
	}
}

func generateProjectDirectory(padpath string) {
	err := os.MkdirAll(padpath, 0755)
	if err != nil {
		log.Fatal(err)
	}

	directories := []string{"apis", "databases", "transforms", "tmp"}
	for _, directory := range directories {
		err := os.Mkdir(filepath.Join(padpath, directory), 0755)
		if err != nil {
			log.Fatal(err)
		}

		_, err = os.Create(filepath.Join(padpath, directory, ".keep"))
		if err != nil {
			log.Fatal(err)
		}
	}

	gitignorefile, err := os.Create(filepath.Join(padpath, ".gitignore"))
	if err != nil {
		log.Fatal(err)
	}
	_, err = gitignorefile.WriteString("tmp/\n")
	if err != nil {
		log.Fatal(err)
	}

	log.WithField("padpath", padpath).Info("Pad generated successfully")
}
