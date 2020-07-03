package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"

	"github.com/stretchr/testify/assert"
)

var (
	testBody          = `{"items": [{"id":1,"name":"Santana"},{"id":2,"name":"David Grohl"}]}`
	testConfiguration = `
Get("%s")
BasicAuth("user", "pass")
ResponseType("json")

LoadStrategy(Full)
TableDefinition({
	"id": "INT",
	"name": "VARCHAR(255)"
})

def Paginate(previous_response):
	return None

def Transform(data):
	return data["items"]

ErrorHandling({
	NetworkError: Retry,
	Http4XXError: Fail,
	Http5XXError: Retry,
	InvalidBodyError: Fail,
})
`
)

func TestExtractLoadAPI(t *testing.T) {
	runAPITest(t, testBody, testConfiguration, func(t *testing.T, portFile string, destdb *sql.DB) {
		redirectLogs(t, func() {
			extractLoadAPI(portFile, "testdest")
			assertRowCount(t, 2, destdb, "test_items")
		})
	})
}

func TestTransformMissingReturn(t *testing.T) {
	configuration := `
Get("%s")
BasicAuth("user", "pass")
ResponseType("json")

LoadStrategy(Full)
TableDefinition({
	"id": "INT",
	"name": "VARCHAR(255)"
})

def Paginate(previous_response):
	return None

def Transform(data):
	return None
	`
	runAPITest(t, testBody, configuration, func(t *testing.T, portFile string, destdb *sql.DB) {
		hook := test.NewGlobal()
		log.SetOutput(ioutil.Discard)
		defer log.SetOutput(os.Stdout)
		log.StandardLogger().ExitFunc = func(int) {}

		extractAPI(portFile)
		assert.Equal(t, log.FatalLevel, hook.LastEntry().Level)
	})
}

func TestAPIPreview(t *testing.T) {
	Preview = true
	runAPITest(t, testBody, testConfiguration, func(t *testing.T, portFile string, destdb *sql.DB) {
		redirectLogs(t, func() {
			expectLogMessage(t, "(not executed)", func() {
				extractLoadAPI(portFile, "testdest")
			})

			assertRowCount(t, 0, destdb, "test_items")
		})
	})
	Preview = false
}

func TestInvalidBodyError(t *testing.T) {
	runAPITest(t, `notjson`, testConfiguration, func(t *testing.T, portFile string, destdb *sql.DB) {
		redirectLogs(t, func() {
			var exitCode ExitCode
			log.StandardLogger().ExitFunc = func(i int) { exitCode = ExitCode(i) }
			extractLoadAPI(portFile, "testdest")
			assert.Equal(t, Fail, exitCode)
		})
	})
}

func TestHTTP5XXError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		fmt.Fprintln(w, `{"message":"Internal Server Error"}`)
	}))
	runAPITest(t, ts, testConfiguration, func(t *testing.T, portFile string, destdb *sql.DB) {
		redirectLogs(t, func() {
			var exitCode ExitCode
			log.StandardLogger().ExitFunc = func(i int) { exitCode = ExitCode(i) }
			extractLoadAPI(portFile, "testdest")
			assert.Equal(t, Retry, exitCode)
		})
	})
}

func TestIncrementalLoadStrategy(t *testing.T) {
	configuration := `
Get("%s")
BasicAuth("user", "pass")
ResponseType("json")

LoadStrategy(Incremental, primary_key="id")
TableDefinition({
	"id": "INT",
	"name": "VARCHAR(255)"
})

def Paginate(previous_response):
	return None

def Transform(data):
	return data["items"]

ErrorHandling({
	NetworkError: Retry,
	Http4XXError: Fail,
	Http5XXError: Retry,
	InvalidBodyError: Fail,
})`

	runAPITest(t, testBody, configuration, func(t *testing.T, portFile string, destdb *sql.DB) {
		destdb.Exec(`CREATE TABLE test_items (id INT, name VARCHAR(255));`)
		destdb.Exec(`INSERT INTO test_items (id, name) VALUES (9, "Bono");`)

		redirectLogs(t, func() {
			extractLoadAPI(portFile, "testdest")
			assertRowCount(t, 3, destdb, "test_items")
		})
	})
}

func TestOffsetPagination(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query()["offset"][0] {
		case "", "0":
			items := `[{"id":1,"name":"Santana"},{"id":2,"name":"David Grohl"}]`
			fmt.Fprintln(w, fmt.Sprintf(`{"items":%s, "offset": 2}`, items))
		case "2":
			items := `[{"id":3,"name":"Jimmy Hendrix"},{"id":4,"name":"Travis Barker"}]`
			fmt.Fprintln(w, fmt.Sprintf(`{"items":%s, "offset": null}`, items))
		}
	}))

	configuration := `
Get("%s?offset={offset}")
ResponseType("json")

LoadStrategy(Full)
TableDefinition({
	"id": "INT",
	"name": "VARCHAR(255)"
})

def Paginate(previous_response):
	if previous_response == None: # For initial request
		return { 'offset': 0 }
	elif previous_response['body']['offset']: # For subsequent requests
		return { 'offset': previous_response['body']['offset'] }
	else: # On final request, stop
		return None

def Transform(data):
	return data["items"]
`
	runAPITest(t, ts, configuration, func(t *testing.T, portFile string, destdb *sql.DB) {
		redirectLogs(t, func() {
			extractLoadAPI(portFile, "testdest")
			assertRowCount(t, 4, destdb, "test_items")
		})
	})
}

func runAPITest(t *testing.T, testServer interface{}, configuration string, testfn func(*testing.T, string, *sql.DB)) {
	var ts *httptest.Server
	switch testServer.(type) {
	case string:
		ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			username, password, ok := r.BasicAuth()
			if ok && username == "user" && password == "pass" {
				fmt.Fprintln(w, testServer.(string))
			} else {
				w.WriteHeader(401)
			}
		}))
	case *httptest.Server:
		ts = testServer.(*httptest.Server)
	}

	tmpFile, err := ioutil.TempFile(os.TempDir(), "/test_items.*port")
	if err != nil {
		t.Fatal("cannot create temporary file:", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err = tmpFile.WriteString(fmt.Sprintf(configuration, ts.URL)); err != nil {
		t.Fatal("failed to write to temporary file:", err)
	}

	Databases["testdest"] = Database{"sqlite://:memory:", map[string]string{}, false}
	db, _ := connectDatabase("testdest")
	defer delete(dbs, "testdest")

	testfn(t, tmpFile.Name(), db)
}
