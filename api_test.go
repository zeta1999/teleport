package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	basicBody  = `[{"id":1,"name":"Santana"},{"id":2,"name":"David Grohl"}]`
	rootedBody = fmt.Sprintf(`{"items":%s}`, basicBody)
)

func TestExtractLoadAPI(t *testing.T) {
	runAPITest(t, basicBody, func(t *testing.T, ts *httptest.Server, destdb *sql.DB) {
		destdb.Exec(`CREATE TABLE test_items (id INT, name VARCHAR(255))`)

		redirectLogs(t, func() {
			extractLoadAPI("test", "testdest", "items", "full", fullStrategyOpts)
			assertRowCount(t, 2, destdb, "test_items")
		})
	})
}

func TestPerformAPIExtraction(t *testing.T) {
	runAPITest(t, basicBody, func(t *testing.T, ts *httptest.Server, destdb *sql.DB) {
		var results []dataObject

		redirectLogs(t, func() {
			api := APIs["test"]
			endpoint := api.Endpoints["items"]
			performAPIExtraction(&api, &endpoint, &results)

			assert.Len(t, results, 2)
			assert.Equal(t, "1", results[0]["id"])
			assert.Equal(t, "2", results[1]["id"])
		})
	})
}

func TestSimpleTransform(t *testing.T) {
	runAPITest(t, rootedBody, func(t *testing.T, ts *httptest.Server, destdb *sql.DB) {
		destdb.Exec(`CREATE TABLE test_items (id INT, name VARCHAR(255))`)
		APITransforms["test/extract_items.star"] = `
def transform(body):
	return body["items"]
`

		endpoints := map[string]Endpoint{"items": Endpoint{"", "GET", make(map[string]string), "json", "none", 1, []string{"test/extract_items.star"}}}
		APIs["test"] = API{ts.URL, make(map[string]string), endpoints}

		// redirectLogs(t, func() {
		extractLoadAPI("test", "testdest", "items", "full", fullStrategyOpts)
		assertRowCount(t, 2, destdb, "test_items")
		// })
	})
}

func TestAPIPreview(t *testing.T) {
	Preview = true
	runAPITest(t, basicBody, func(t *testing.T, ts *httptest.Server, destdb *sql.DB) {
		destdb.Exec(`CREATE TABLE test_items (id INT, name VARCHAR(255))`)

		redirectLogs(t, func() {
			expectLogMessage(t, "(not executed)", func() {
				extractLoadAPI("test", "testdest", "items", "full", fullStrategyOpts)
			})

			assertRowCount(t, 0, destdb, "test_items")
		})
	})
	Preview = false
}

func runAPITest(t *testing.T, body string, testfn func(*testing.T, *httptest.Server, *sql.DB)) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, body)
	}))
	endpoints := map[string]Endpoint{"items": Endpoint{"", "GET", make(map[string]string), "json", "none", 1, []string{}}}
	APIs["test"] = API{ts.URL, make(map[string]string), endpoints}
	defer ts.Close()

	Databases["testdest"] = Database{"sqlite://:memory:", map[string]string{}, false}
	db, _ := connectDatabase("testdest")
	defer delete(dbs, "testdest")

	testfn(t, ts, db)
}
