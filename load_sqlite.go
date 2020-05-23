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
)

func importSqlite3(database *sql.DB, table string, file string, columns []Column) {
	transaction, err := database.Begin()
	if err != nil {
		log.Fatal(err)
	}

	preparedStatement := fmt.Sprintf("INSERT INTO %s (", table)
	for _, column := range columns {
		preparedStatement += fmt.Sprintf("%s, ", column.Name)
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
			log.Fatal("Import statement exec error: ", err)
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
