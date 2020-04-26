package main

import (
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
	"github.com/xo/dburl"

	"github.com/hundredwatt/teleport/cli"
)

func main() {
	opts := cli.ParseArguments()

	switch opts.Command {
	case "extract":
		extract(opts.DataSource, opts.TableName)
	}
}

func extract(_ string, table string) {
	database, _ := dburl.Open("sqlite3://test/example.sqlite3")
	res, err := database.Query("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		log.Println(err)
		return
	}
	var name string
	for res.Next() {
		res.Scan(&name)
		fmt.Println(name)
	}
}
