package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	testPlugin        = "cron"
	testPluginTarball = "testdata/teleport-cron.tar.gz"
)

func TestInstallAndCallPlugin(t *testing.T) {
	os.Setenv("PADPATH", "testdata/pad")
	defer os.Unsetenv("PADPATH")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bytes, err := ioutil.ReadFile(testPluginTarball)
		if err != nil {
			t.Fatal(err)
		}
		w.Write(bytes)
	}))

	plugins["cron"] = ts.URL

	var err error

	out := captureOutput(t, func() {
		err = InstallPlugin(testPlugin)
	})
	defer os.RemoveAll("testdata/pad/.plugins")
	assert.NoError(t, err)
	assert.True(t, PluginInstalled(testPlugin))
	assert.Contains(t, out, "plugin installed ✓")
	assert.Contains(t, out, fmt.Sprintf("teleport plugin -- %s", testPlugin)) // Post install message

	out = captureOutput(t, func() {
		err = CallPlugin(testPlugin, []string{})
	})
	assert.NoError(t, err)
	assert.Contains(t, out, "PADPATH=")
	assert.Contains(t, out, "/testdata/pad")
	assert.Contains(t, out, "*/1 * * * /usr/local/bin/teleport extract-load-api -from worldtimeapi_ip_times -to postgresdocker")
	assert.Len(t, strings.Split(out, "\n"), 7+1)
}

func captureOutput(t *testing.T, f func()) string {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	stdout := os.Stdout
	os.Stdout = w
	defer func() {
		os.Stdout = stdout
	}()

	f()
	w.Close()

	var buf bytes.Buffer
	io.Copy(&buf, r)

	return buf.String()
}
