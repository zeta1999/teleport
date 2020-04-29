package main

import (
	"log"

	_ "github.com/mattn/go-sqlite3"
	"github.com/xo/dburl"

	"github.com/hundredwatt/teleport/cli"
)

func main() {
	opts := cli.ParseArguments()
	readConnections()

	switch opts.Command {
	case "extract":
		extract(opts.DataSource, opts.TableName)
	}
}

func extract(source string, table string) {
	url := Connections[source].Config.Url
	database, err := dburl.Open(url)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}
}
