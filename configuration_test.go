package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.starlark.net/starlark"
)

func TestPadStructure(t *testing.T) {
	var (
		secretKey = "SAMPLE90odbSuT8aS12nUFjPlUg1ip0AAeQg0wiJKzv318auuDh0C6zKDmTrfqWwqGvA0O"
		key       = "USER_TOKEN"
		value     = "SAMPLELq6FdAp3Rjv"
	)

	redirectLogs(t, func() {
		os.Setenv("PADPATH", "testdata/pad")
		os.Setenv("TELEPORT_SECRET_KEY", secretKey)
		defer os.Unsetenv("PADPATH")
		defer os.Unsetenv("TELEPORT_SECRET_KEY")
		defer os.Unsetenv(key)

		currentWorkflow = &Workflow{Thread: &starlark.Thread{}}
		var endpoint Endpoint

		// API files in ./sources/apis
		err := readEndpointConfiguration("example_widgets", &endpoint)
		assert.NoError(t, err)
		assert.Equal(t, "http://127.0.0.1:4567/widgets.json", endpoint.URL)

		// Database connection configuration in ./config/databases.EXT with cleanenv file extensions (.json, .yaml, .edn)
		readDatabaseConnectionConfiguration()
		assert.Equal(t, "sqlite3://testdata/pad/tmp/example.sqlite3", Databases["example"].URL)

		// Database extract configuration in ./sources/databases with .port extension
		var tableExtract TableExtract
		err = readTableExtractConfiguration("example", "objects", &tableExtract)
		assert.NoError(t, err)
		assert.Equal(t, Full, tableExtract.LoadOptions.Strategy)

		// Port files can have .port or .port.py extensions
		err = readEndpointConfiguration("worldtimeapi_ip_times", &endpoint)
		assert.NoError(t, err)
		assert.Equal(t, "http://worldtimeapi.org/api/ip", endpoint.URL)

		// Secrets file at ./config/secrets.txt.enc
		setEnvironmentValuesFromSecretsFile()
		userTokenValue, ok := os.LookupEnv(key)
		assert.Equal(t, true, ok)
		assert.Equal(t, value, userTokenValue)
	})
}

func TestLegacyPadStructure(t *testing.T) {
	var (
		secretKey = "SAMPLE90odbSuT8aS12nUFjPlUg1ip0AAeQg0wiJKzv318auuDh0C6zKDmTrfqWwqGvA0O"
		key       = "USER_TOKEN"
		value     = "SAMPLELq6FdAp3Rjv"
	)

	redirectLogs(t, func() {
		os.Setenv("PADPATH", "testdata/legacy_pad")
		os.Setenv("TELEPORT_SECRET_KEY", secretKey)
		defer os.Unsetenv("PADPATH")
		defer os.Unsetenv("TELEPORT_SECRET_KEY")
		defer os.Unsetenv(key)

		currentWorkflow = &Workflow{Thread: &starlark.Thread{}}

		// API files in ./apis
		var endpoint Endpoint
		err := readEndpointConfiguration("example_widgets", &endpoint)
		assert.NoError(t, err)
		assert.Equal(t, "http://127.0.0.1:4567/widgets.json", endpoint.URL)

		// Database connection configuration in ./databases with cleanenv file extensions (.json, .yaml, .edn)
		expectLogMessage(t, "open testdata/legacy_pad/config: no such file or directory", func() {
			readDatabaseConnectionConfiguration()
		})
		assert.Equal(t, "sqlite3://testdata/legacy_pad/tmp/example.sqlite3", Databases["example"].URL)

		// Database extract configuration in ./databases with .port extension
		var tableExtract TableExtract
		err = readTableExtractConfiguration("example", "objects", &tableExtract)
		assert.NoError(t, err)
		assert.Equal(t, Full, tableExtract.LoadOptions.Strategy)

		// Secrets file at ./secrets.txt
		setEnvironmentValuesFromSecretsFile()
		userTokenValue, ok := os.LookupEnv(key)
		assert.Equal(t, true, ok)
		assert.Equal(t, value, userTokenValue)
	})
}
