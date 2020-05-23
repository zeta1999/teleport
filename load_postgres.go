package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"io"
	"log"
	"os"

	"github.com/lib/pq"
)

func importPostgres(database *sql.DB, table string, file string, columns []Column) {
	transaction, err := database.Begin()
	if err != nil {
		log.Fatal(err)
	}

	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
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
