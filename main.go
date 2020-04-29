package main

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/jimsmart/schema"
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

	tables, err := schema.TableNames(database)
	if err != nil {
		log.Fatal(err)
	}
	targetTable := ""
	for _, tablename := range tables {
		if tablename == table {
			targetTable = tablename
		}
	}
	if targetTable == "" {
		log.Fatalf("table \"%s\" not found in \"%s\"", table, source)
	}

	tmpfile, err := ioutil.TempFile("/tmp/", fmt.Sprintf("extract-%s-%s", table, source))
	if err != nil {
		log.Fatal(err)
	}

	rows, err := database.Query(fmt.Sprintf("SELECT * FROM %s", targetTable))
	writer := csv.NewWriter(tmpfile)
	columnNames, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}
	writer.Write(columnNames)

	rawResult := make([]interface{}, len(columnNames))
	writeBuffer := make([]string, len(columnNames))
	for i := range writeBuffer {
		rawResult[i] = &writeBuffer[i]
	}

	for rows.Next() {
		err := rows.Scan(rawResult...)
		if err != nil {
			log.Fatal(err)
		}

		err = writer.Write(writeBuffer)
		if err != nil {
			log.Fatal(err)
		}
	}

	//defer os.Remove(tmpfile.Name())
	writer.Flush()

	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}
}
