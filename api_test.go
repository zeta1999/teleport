package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractLoadAPI(t *testing.T) {
	runAPITest(t, func(t *testing.T, ts *httptest.Server, destdb *sql.DB) {
		destdb.Exec(`CREATE TABLE test_objects (id INT, name VARCHAR(255))`)

		redirectLogs(t, func() {
			extractLoadAPI("test", "testdest", "objects", "full", fullStrategyOpts)
			assertRowCount(t, 2, destdb, "test_objects")
		})
	})

}

func TestPerformAPIExtraction(t *testing.T) {
	runAPITest(t, func(t *testing.T, ts *httptest.Server, destdb *sql.DB) {
		task := taskContext{"test", "", "", "full", fullStrategyOpts, nil, nil, "", nil, nil}

		performAPIExtraction(&task)
		results := *task.Results

		assert.Len(t, results, 2)
		assert.Equal(t, "1", results[0]["id"])
		assert.Equal(t, "2", results[1]["id"])
	})
}

func runAPITest(t *testing.T, testfn func(*testing.T, *httptest.Server, *sql.DB)) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[{"id":1,"name":"Santana"},{"id":2,"name":"David Grohl"}]`)
	}))
	Endpoints["test"] = Endpoint{"test", "GET", ts.URL, make(map[string]string), "json", "none", 1, make([]string, 0)}
	defer ts.Close()

	Connections["testdest"] = Connection{"testdest", Configuration{"sqlite://:memory:", map[string]string{}}}
	db, _ := connectDatabase("testdest")
	defer delete(dbs, "testdest")

	testfn(t, ts, db)
}
