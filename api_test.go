package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractLoadAPI(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[{"id":1,"name":"Santana"},{"id":2,"name":"David Grohl"}]`)
	}))
	defer ts.Close()
	Endpoints["test"] = Endpoint{"test", "GET", ts.URL, make(map[string]string), "json", "none", 1, make([]string, 0)}

	Connections["test1"] = Connection{"test1", Configuration{"sqlite://:memory:", map[string]string{}}}
	db, _ := connectDatabase("test1")
	db.Exec(`CREATE TABLE test_objects (id INT, name VARCHAR(255))`)

	extractLoadAPI("test", "test1", "objects", "full", fullStrategyOpts)
	assertRowCount(t, 2, db, "test_objects")

	db.Exec("DROP TABLE test_objects;")
}

func TestPerformAPIExtraction(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[{"id":1,"name":"Santana"},{"id":2,"name":"David Grohl"}]`)
	}))
	defer ts.Close()
	Endpoints["test"] = Endpoint{"test", "GET", ts.URL, make(map[string]string), "json", "none", 1, make([]string, 0)}

	task := taskContext{"test", "", "", "full", fullStrategyOpts, nil, nil, "", nil, nil}

	performAPIExtraction(&task)
	results := *task.Results

	assert.Len(t, results, 2)
	assert.Equal(t, "1", results[0]["id"])
	assert.Equal(t, "2", results[1]["id"])
}
