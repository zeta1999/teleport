package cli

import (
	"flag"
	"log"
	"os"
)

type CliOptions struct {
	Command    string
	DataSource string
	TableName  string
	File       string
}

func ParseArguments() CliOptions {
	options := CliOptions{}

	flag.Usage = help
	options.Command = os.Args[1]
	flag.StringVar(&options.DataSource, "s", "", "Data source name")
	flag.StringVar(&options.TableName, "t", "", "Name of table from data source")
	flag.StringVar(&options.File, "f", "", "Path to file to be used in command")

	versionPtr := flag.Bool("v", false, "Show version")
	flag.CommandLine.Parse(os.Args[2:])
	if *versionPtr {
		os.Exit(0)
	}

	return options
}

func help() {
	log.Println("-s [source]\tData source name")
	log.Println("-t [table]\tName of table from data source")
	log.Println("-v\t\tShow version and license information")
	log.Println("-h\t\tThis help screen")
}
