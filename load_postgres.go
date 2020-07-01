package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"io"
	"os"

	"github.com/hundredwatt/teleport/schema"
	"github.com/lib/pq"
)

func importPostgres(database *sql.DB, table string, file string, columns []schema.Column) error {
	transaction, err := database.Begin()
	if err != nil {
		return err
	}

	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
	}

	statement, err := transaction.Prepare(pq.CopyIn(table, columnNames...))
	if err != nil {
		return err
	}

	csvfile, err := os.Open(file)
	if err != nil {
		return err
	}

	reader := csv.NewReader(bufio.NewReader(csvfile))

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
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
			return err
		}
	}

	_, err = statement.Exec()
	if err != nil {
		return err
	}

	err = statement.Close()
	if err != nil {
		return err
	}

	err = transaction.Commit()
	if err != nil {
		return err
	}

	return nil
}
