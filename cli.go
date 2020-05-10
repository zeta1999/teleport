package main

import (
	"flag"
	"fmt"
	"os"
)

type CliOptions struct {
	Command    string
	Source     string
	FromSource string
	ToSource   string
	TableName  string
	File       string
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
