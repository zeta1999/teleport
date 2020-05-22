package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

type CliOptions struct {
	Command          string
	Source           string
	FromSource       string
	ToSource         string
	TableName        string
	File             string
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
	flag.StringVar(&options.Strategy, "strategy", "full", "data update strategy to be used when extracting and/or loading (full, incremental)")
	flag.StringVar(&options.PrimaryKey, "primary-key", "id", "column name of primary key to be used when updating data")
	flag.StringVar(&options.ModifiedAtColumn, "modified-at-column", "updated_at", "column name of modified_at column to be used with incremental strategy")
	flag.StringVar(&options.HoursAgo, "hours-ago", "36", "set the number of hours to look back for modified records")

	versionPtr := flag.Bool("v", false, "Show version")
	flag.CommandLine.Parse(os.Args[2:])
	if *versionPtr {
		os.Exit(0)
	}

	return options
}

func help() {
	fmt.Println("-source, -s [source]\tData source name")
	fmt.Println("-from [source]\tData source to extract data from")
	fmt.Println("-to [source]\tData source to load data into")
	fmt.Println("-table, -t [table]\tName of table from data source")
	fmt.Println("-v\t\tShow version and license information")
	fmt.Println("-h\t\tThis help screen")
}

func extractStrategyOptions(opts *CliOptions) (strategyOpts map[string]string) {
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

	return
}
