package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicExtractAPI(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[{"id":1,"name":"Santana"},{"id":2,"name":"David Grohl"}]`)
	}))
	defer ts.Close()
	Endpoints["test"] = Endpoint{"test", "GET", ts.URL, make(map[string]string), "json", "none", 1, make([]string, 0)}

	csvfile, _ := performAPIExtraction("test")
	fmt.Println(csvfile)

	assertCsvRowCount(t, 2, csvfile)
	// assertCsvCellContents(t, "id", csvfile, 0, 0)
	assertCsvCellContents(t, "1", csvfile, 0, 0)
	assertCsvCellContents(t, "Santana", csvfile, 0, 1)
	assertCsvCellContents(t, "David Grohl", csvfile, 1, 1)
}

func assertCsvRowCount(t *testing.T, expected int, csvfilename string) {
	csvfile, err := os.Open(csvfilename)
	if err != nil {
		panic(err)
	}

	reader := csv.NewReader(bufio.NewReader(csvfile))

	rowItr := 0

	for {
		_, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			panic(error)
		}

		rowItr++
	}

	assert.EqualValues(t, expected, rowItr)
}
