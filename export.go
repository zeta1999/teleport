package main

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"
)

func exportCSV(source string, table string, columns []Column) (string, error) {
	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	if !tableExists(source, table) {
		log.Fatalf("table \"%s\" not found in \"%s\"", table, source)
	}

	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
	}

	tmpfile, err := ioutil.TempFile("/tmp/", fmt.Sprintf("extract-%s-%s", table, source))
	if err != nil {
		log.Fatal(err)
	}

	rows, err := database.Query(fmt.Sprintf("SELECT %s FROM %s", strings.Join(columnNames, ", "), table))
	if err != nil {
		log.Fatal(err)
	}
	writer := csv.NewWriter(tmpfile)
	columnNames, err = rows.Columns()
	if err != nil {
		log.Fatal(err)
	}

	destination := make([]interface{}, len(columnNames))
	rawResult := make([]interface{}, len(columnNames))
	writeBuffer := make([]string, len(columnNames))
	for i := range rawResult {
		destination[i] = &rawResult[i]
	}
	for rows.Next() {
		err := rows.Scan(destination...)
		if err != nil {
			log.Fatal(err)
		}

		for i := range columns {
			switch rawResult[i].(type) {
			case time.Time:
				writeBuffer[i] = rawResult[i].(time.Time).Format("2006-01-02 15:04:05")
			case int64:
				writeBuffer[i] = strconv.FormatInt(rawResult[i].(int64), 10)
			case string:
				writeBuffer[i] = rawResult[i].(string)
			case float64:
				writeBuffer[i] = strconv.FormatFloat(rawResult[i].(float64), 'E', -1, 64)
			case nil:
				writeBuffer[i] = ""
			default:
				writeBuffer[i] = string(rawResult[i].([]byte))
			}
		}

		err = writer.Write(writeBuffer)
		if err != nil {
			log.Fatal(err)
		}
	}

	writer.Flush()

	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	return tmpfile.Name(), nil
}
