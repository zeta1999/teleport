package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/jimsmart/schema"
	"github.com/lib/pq"
)

func importCSV(source string, table string, file string) {
	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	switch driverType := fmt.Sprintf("%T", database.Driver()); driverType {
	case "*pq.Driver":
		importPostgres(database, table, file)
	case "*sqlite3.SQLiteDriver":
		importSqlite3(database, table, file)
	}
}

func importPostgres(database *sql.DB, table string, file string) {
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

func importSqlite3(database *sql.DB, table string, file string) {
	columns, err := schema.Table(database, table)
	if err != nil {
		log.Fatalf("Table error: %s", err)
	}

	transaction, err := database.Begin()
	if err != nil {
		log.Fatal(err)
	}

	preparedStatement := fmt.Sprintf("INSERT INTO %s (", table)
	for _, column := range columns {
		preparedStatement += fmt.Sprintf("%s, ", column.Name())
	}
	preparedStatement = strings.TrimSuffix(preparedStatement, ", ")

	preparedStatement += ") VALUES ("
	for range columns {
		preparedStatement += "?, "
	}
	preparedStatement = strings.TrimSuffix(preparedStatement, ", ")
	preparedStatement += ");"

	statement, err := transaction.Prepare(preparedStatement)
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
			log.Fatalf("Line error: %s", error)
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
			log.Fatalf("Statement exec error: %s", error)
		}
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
