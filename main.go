package main

import (
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/go-sql-driver/mysql"
	"github.com/hundredwatt/teleport/schema"
	"github.com/hundredwatt/teleport/secrets"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
)

var (
	// Preview indicates preview/dry-run mode is enabled
	Preview bool = false

	// PreviewLimit sets the number of rows to output while in preview mode
	PreviewLimit int = 3

	// Version sets the release version
	Version string

	// Build sets the build tag
	Build string

	// SecretsFile sets the location of the secrets file in the Pad directory
	SecretsFile string = "config/secrets.txt.enc"

	legacySecretsFile string = "secrets.txt"
)

func main() {
	if _, ok := os.LookupEnv("PADPATH"); !ok {
		os.Setenv("PADPATH", ".")
	}

	if len(os.Args) == 1 {
		help()
		return
	}

	switch os.Args[1] {

	// Basic CLI commands
	case "version", "-v":
		version()
		return
	case "help", "-h", "--help":
		help()
		return
	case "new":
		if len(os.Args) == 3 {
			generateProjectDirectory(os.Args[2])
		} else {
			fmt.Println("Wrong number of options provided to `teleport new`")
			fmt.Println("Syntax:")
			fmt.Println("  teleport new </path/to/pad-name>")
		}
		return
	}

	opts := parseArguments()
	Preview = opts.Preview

	if Preview || opts.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	setEnvironmentValuesFromSecretsFile()
	readDatabaseConnectionConfiguration()

	switch opts.Command {

	// Secrets
	case "secrets":
		secretsCLI()
		return

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
		database, err := connectDatabase(opts.Source)
		if err != nil {
			log.Fatal(err)
		}

		table, err := schema.DumpTableMetadata(database, opts.TableName)
		if err != nil {
			log.Fatal(err)
		}
		importCSV(opts.Source, opts.TableName, opts.File, table.Columns)

	// Extract data from a source to csv
	case "extract", "extract-db":
		extractDatabase(opts.FromSource, opts.TableName)
	case "extract-api":
		extractAPI(opts.FromSource)

	// Extract data from a source and load into datawarehouse
	case "extract-load", "extract-load-db":
		extractLoadDatabase(opts.FromSource, opts.ToSource, opts.TableName)
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

	directories := []string{"config", "sources", "sources/apis", "sources/databases", "transforms", "tmp"}
	for _, directory := range directories {
		err := os.Mkdir(filepath.Join(padpath, directory), 0755)
		if err != nil {
			log.Fatal(err)
		}

		// No .keep file in sources/
		if directory == "sources" {
			continue
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

	// err = secrets.InitializeSecretsFile(secretsSettings())
	// if err != nil {
	// 	log.Fatal(err)
	// }

	databasesConfigFile, err := os.Create(filepath.Join(padpath, "config", "databases.yml"))
	if err != nil {
		log.Fatal(err)
	}
	_, err = databasesConfigFile.WriteString("# db1:\n#   url: postgres://$USER:$PASS@$HOST/$DBNAME")
	if err != nil {
		log.Fatal(err)
	}

	log.WithField("padpath", padpath).Info("Pad generated successfully")
}

func secretsSettings() secrets.Settings {
	secretsFilePath := filepath.Join(os.Getenv("PADPATH"), SecretsFile)

	if _, err := os.Stat(filepath.Join(os.Getenv("PADPATH"), legacySecretsFile)); err == nil {
		secretsFilePath = filepath.Join(os.Getenv("PADPATH"), legacySecretsFile)
	}

	return secrets.Settings{
		"TELEPORT (https://github.com/hundredwatt/teleport)",
		"TELEPORT_SECRET_KEY",
		secretsFilePath,
	}
}

func setEnvironmentValuesFromSecretsFile() {
	settings := secretsSettings()

	_, err := os.Stat(settings.SecretsFile)
	if err != nil {
		// secrets file not found
		return
	}

	body, err := secrets.ReadSecretsFile(secretsSettings())
	if err != nil {
		log.Warnf("unable to decrypt secrets file: %s", err)
	}

	for _, variable := range body {
		os.Setenv(variable.Key, variable.Value)
	}
}
