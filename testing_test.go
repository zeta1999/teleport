package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hundredwatt/teleport/schema"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestTestingRecordsCassetteAndResults(t *testing.T) {
	cases := []string{"api_basic_auth.port", "api_header_auth.port", "api_csv.port"}
	for _, cse := range cases {
		dir, err := ioutil.TempDir("/tmp", "testing")
		failNowOnError(t, err)
		defer os.RemoveAll(dir)

		os.Setenv("PADPATH", dir)
		defer os.Unsetenv("PADPATH")

		runAPITest(t, cse, func(t *testing.T, portFile string, _ *schema.Database) {
			testAPI(portFile)

			assert.FileExists(t, filepath.Join(dir, "testing", "api_recordings", "test_items", "cassette.yaml"))
			assert.FileExists(t, filepath.Join(dir, "testing", "api_recordings", "test_items", "results.csv"))

			cassette_bytes, err := ioutil.ReadFile(filepath.Join(dir, "testing", "api_recordings", "test_items", "cassette.yaml"))
			failNowOnError(t, err)
			cassette_contents := string(cassette_bytes)
			fmt.Println(cassette_contents)

			// Hides environment variables in URL
			assert.NotContains(t, cassette_contents, "http://")
			assert.NotContains(t, cassette_contents, "$TEST_URL")
			assert.Contains(t, cassette_contents, strings.Repeat("*", len(os.Getenv("TEST_URL"))))

			switch cse {
			case "api_basic_auth.port":
				// Strips Basic Auth
				assert.NotContains(t, cassette_contents, "Authorization")
				assert.NotContains(t, cassette_contents, "Basic dXNlcjpwYXNz")

				// Anonymizes strings
				assert.NotContains(t, cassette_contents, "Santana")
				assert.Contains(t, cassette_contents, "S******")

				assert.NotContains(t, cassette_contents, "David")
				assert.NotContains(t, cassette_contents, "Grohl")
				assert.Contains(t, cassette_contents, "D****")
				assert.Contains(t, cassette_contents, "G****")
			case "api_header_auth.port":
				// Strips Authorization Header
				assert.NotContains(t, cassette_contents, "Authorization")
				assert.NotContains(t, cassette_contents, "Bearer 292b0e")

				// Anonymizes strings
				assert.NotContains(t, cassette_contents, "Santana")
				assert.Contains(t, cassette_contents, "S******")

				assert.NotContains(t, cassette_contents, "David")
				assert.NotContains(t, cassette_contents, "Grohl")
				assert.Contains(t, cassette_contents, "D****")
				assert.Contains(t, cassette_contents, "G****")

			case "api_csv.port":
				// Does not anonymize headers
				assert.NotContains(t, cassette_contents, "p****")
				assert.Contains(t, cassette_contents, "price")

				// Anonymizes non-header row celss
				// assert.NotContains(t, cassette_contents, "David")
				// assert.NotContains(t, cassette_contents, "Grohl")
				// assert.Contains(t, cassette_contents, "D****")
				// assert.Contains(t, cassette_contents, "G****")

			}

			// Passes when using recording
			testAPI(portFile)
			assert.Equal(t, log.InfoLevel, hook.LastEntry().Level)
		})
	}
}
