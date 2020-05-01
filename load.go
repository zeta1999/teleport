package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/jimsmart/schema"
	"github.com/lib/pq"
	"github.com/xo/dburl"
)

func load(source string, table string, file string) {
	url := Connections[source].Config.Url
	database, err := dburl.Open(url)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	switch driverType := fmt.Sprintf("%T", database.Driver()); driverType {
	case "*pq.Driver":
		loadPostgres(source, table, file)
	case "*sqlite3.SQLiteDriver":
		loadSqlite3(source, table, file)
	}
}

func loadPostgres(source string, table string, file string) {
	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	transaction, err := database.Begin()
	if err != nil {
		log.Fatal(err)
	}

	columns, err := schema.Table(database, table)
	if err != nil {
		log.Fatal(err)
	}

	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name()
	}

	statement, err := transaction.Prepare(pq.CopyIn(table, columnNames...))
	if err != nil {
		log.Fatal(err)
	}

	csvfile, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	reader := csv.NewReader(bufio.NewReader(csvfile))
	if err != nil {
		log.Fatal(err)
	}

	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}

		writeBuffer := make([]interface{}, len(line))
		for i, value := range line {
			if value == "" { // Assume a blank cell is NULL
				writeBuffer[i] = nil
			} else {
				writeBuffer[i] = value
			}
		}

		_, err = statement.Exec(writeBuffer...)
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err = statement.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = statement.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = transaction.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

func loadSqlite3(source string, table string, file string) {
	url := Connections[source].Config.Url
	sqlite3URL, err := dburl.Parse(url)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	cmd := exec.Command("sqlite3", sqlite3URL.DSN)
	cmd.Stdin = strings.NewReader(fmt.Sprintf(".mode csv\n.import %s %s", file, table))
	var errout bytes.Buffer
	cmd.Stderr = &errout
	err = cmd.Run()
	fmt.Println(cmd.ProcessState)
	if err != nil {
		log.Fatal("CSV Import Error: ", errout.String())
	}
}
