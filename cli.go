package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

type CliOptions struct {
	Command    string
	Source     string
	FromSource string
	ToSource   string
	TableName  string
	File       string
	Preview    bool
	Debug      bool
}

type StrategyOptions struct {
	Strategy         string
	PrimaryKey       string
	ModifiedAtColumn string
	HoursAgo         string
}

func parseArguments() CliOptions {
	options := CliOptions{}

	flag.Usage = help
	options.Command = os.Args[1]
	flag.StringVar(&options.Source, "source", "", "Name of data source to perform command on")
	flag.StringVar(&options.Source, "s", "", "Alias for -source")
	flag.StringVar(&options.FromSource, "from", "", "Data source to extract data from")
	flag.StringVar(&options.ToSource, "to", "", "Data source to load data into")
	flag.StringVar(&options.TableName, "table", "", "Name of table to perform operations on")
	flag.StringVar(&options.TableName, "t", "", "Alias for -table")
	flag.StringVar(&options.File, "file", "", "Path to file to be used in command")
	flag.StringVar(&options.File, "f", "", "Alias for -file")
	flag.BoolVar(&options.Preview, "p", false, "alias for -preview")
	flag.BoolVar(&options.Preview, "preview", options.Preview, "use preview mode to perform a dry-run with truncated data and verbose logging")
	flag.BoolVar(&options.Debug, "d", false, "alias for -debug")
	flag.BoolVar(&options.Debug, "debug", options.Debug, "enable debug log messages")

	flag.CommandLine.Parse(os.Args[2:])

	return options
}

func parseStrategyOptions() (strategyOpts StrategyOptions) {
	flag.StringVar(&strategyOpts.Strategy, "strategy", "full", "data update strategy to be used when extracting and/or loading (full, modified-only)")
	flag.StringVar(&strategyOpts.PrimaryKey, "primary-key", "id", "column name of primary key to be used when updating data")
	flag.StringVar(&strategyOpts.ModifiedAtColumn, "modified-at-column", "updated_at", "column name of modified_at column to be used with modified-only strategy")
	flag.StringVar(&strategyOpts.HoursAgo, "hours-ago", "36", "set the number of hours to look back for modified records")
	flag.CommandLine.Parse(os.Args[2:])

	switch strategyOpts.Strategy {
	case "full", "modified-only":
	default:
		log.Fatal("Invalid strategy, acceptable options: full, modified-only")
	}

	return
}

func help() {
	fmt.Println("usage: teleport [COMMAND] [OPTIONS]")
	fmt.Println("Commands:")
	fmt.Println("  new <path/to/pad>\tgenerate a new pad folder at the given path")
	fmt.Println("  help\t\t\tshow this message")
	fmt.Println("  version\t\tprint version information")
	fmt.Println("")
	fmt.Println("  extract-db\t\texport data from a database table to CSV. Required options: -from, -table")
	fmt.Println("  extract-api\t\texport data from an API endpoint to CSV. Required options: -from")
	fmt.Println("")
	fmt.Println("  extract-load-db\t\texport data from a table in one database to another. Required options: -from, -to, -table")
	fmt.Println("  extract-load-api\t\texport data from an API endpoint to a database. Required options: -from, -to")
	fmt.Println("")
	fmt.Println("  transform\t\t(re-)generate a materialized table form a sql statement. Required options: -source, -table")
	fmt.Println("")
	fmt.Println("  about-db\t\tshow connection information a database. Required options: -source")
	fmt.Println("  db-terminal\t\tstart a terminal for interacting with a database. Required options: -source")
	fmt.Println("  list-tables\t\tlist the tables in a database. Required options: -source")
	fmt.Println("  drop-table\t\tdrop a table. Required options: -source, -table")
	fmt.Println("  describe-table\tprint the schema for a table. Required options: -source, -table")
	fmt.Println("")
	fmt.Println("Options:")
	fmt.Println("  -source, -s [source]\tdata source name")
	fmt.Println("  -from [source]\tdata source to extract data from")
	fmt.Println("  -to [source]\t\tdata source to load data into")
	fmt.Println("  -table, -t [table]\tname of table in the database data source")
	fmt.Println("  -preview, -p\t\tpreview command as a dry-run without making any changes")
	fmt.Println("  -debug, -d\t\tenable debug log output")
}

func version() {
	fmt.Fprintln(os.Stderr, fmt.Sprintf("Teleport %s (build: %s)", Version, Build))
}

func listCommands() {
	fmt.Println("")
	fmt.Println("Teleport commands")
	fmt.Println("new\thelp\tversion")
	fmt.Println("")
	fmt.Println("Extract commands")
	fmt.Println("extract-db\textract-api")
	fmt.Println("")
	fmt.Println("Extract and load commands")
	fmt.Println("extract-load-db\textract-load-api")
	fmt.Println("")
	fmt.Println("Transform commands")
	fmt.Println("transform")
	fmt.Println("")
	fmt.Println("Database commands")
	fmt.Println("about-db\tdb-terminal\tlist-tables")
	fmt.Println("drop-table\tdescribe-table")
}
