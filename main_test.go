package main

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

var (
	hook = test.NewGlobal()
)

func init() {
	log.SetLevel(log.DebugLevel)

	ResetCurrentWorkflow()
}

func expectLogMessage(t *testing.T, message string, fn func()) {
	expectLogMessages(t, []string{message}, fn)
}

func expectLogMessages(t *testing.T, messages []string, fn func()) {
	redirectLogs(t, fn)

	for _, message := range messages {
		for _, entry := range hook.Entries {
			logMessage, err := entry.String()
			assert.NoError(t, err)

			if strings.Contains(logMessage, message) {
				return
			}
		}
	}

	assert.Fail(t, "%s not found in logs", messages)
}

func redirectLogs(t *testing.T, fn func()) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	defer log.SetLevel(log.GetLevel())
	log.SetLevel(log.DebugLevel)

	log.StandardLogger().ExitFunc = func(int) {}

	hook.Reset()

	fn()

	for _, entry := range hook.Entries {
		t.Log(entry.String())
	}
}

func failNowOnError(t *testing.T, err error) {
	if !assert.NoError(t, err) {
		t.FailNow()
	}
}
