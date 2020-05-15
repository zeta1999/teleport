package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExportTimestamp(t *testing.T) {
	Connections["test1"] = Connection{"test1", Configuration{"sqlite://:memory:", map[string]string{}}}
	db1, _ := connectDatabase("test1")

	columns := make([]Column, 0)
	columns = append(columns, Column{"created_at", TIMESTAMP, map[Option]int{}})
	table := Table{"test1", "timestamps", columns}

	db1.Exec(table.generateCreateTableStatement("timestamps"))
	db1.Exec("INSERT INTO timestamps (created_at) VALUES (DATETIME(1092941466, 'unixepoch'))")
	db1.Exec("INSERT INTO timestamps (created_at) VALUES (NULL)")

	tempfile, _ := exportCSV("test1", "timestamps", columns)

	assertCsvCellContents(t, "2004-08-19 18:51:06", tempfile, 0, 0)
	assertCsvCellContents(t, "", tempfile, 1, 0)

	db1.Exec("DROP TABLE timestamps;")
}

func assertCsvCellContents(t *testing.T, expected string, csvfilename string, row int, col int) {
	csvfile, err := os.Open(csvfilename)
	if err != nil {
		panic(err)
	}

	reader := csv.NewReader(bufio.NewReader(csvfile))

	rowItr := 0

	for {
		line, error := reader.Read()
		if error == io.EOF {
			assert.FailNow(t, "fewer than %d rows in CSV", row)
		} else if error != nil {
			panic(error)
		}

		if row != rowItr {
			rowItr++
			break
		}

		assert.EqualValues(t, expected, line[col])
		return
	}
}
