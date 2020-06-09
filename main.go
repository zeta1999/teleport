package main

import (
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

	// Extract data from a source to csv
	case "extract":
		extractDatabase(opts.FromSource, opts.TableName)
	case "extract-api":
		extractAPI(opts.FromSource, opts.EndpointName)

	// Extract data from a source and load into datawarehouse
	case "extract-load":
		extractLoadDatabase(opts.FromSource, opts.ToSource, opts.TableName, opts.Strategy, extractStrategyOptions(&opts))
	case "extract-load-api":
		extractLoadAPI(opts.FromSource, opts.ToSource, opts.EndpointName, opts.Strategy, extractStrategyOptions(&opts))

	// Run Transform within datawarehouse
	case "transform":
		updateTransform(opts.Source, opts.TableName)
	}
}

func generateProjectDirectory(path string) {
	pad := filepath.Join(workingDir(), path)

	err := os.MkdirAll(pad, 0755)
	if err != nil {
		log.Fatal(err)
	}

	directories := []string{"apis", "apis/transforms", "databases", "transforms"}
	for _, directory := range directories {
		err := os.Mkdir(filepath.Join(pad, directory), 0755)
		if err != nil {
			log.Fatal(err)
		}

		_, err = os.Create(filepath.Join(pad, directory, ".keep"))
		if err != nil {
			log.Fatal(err)
		}
	}

	log.WithField("padpath", pad).Info("Pad generated successfully")
}
