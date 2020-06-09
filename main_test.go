package main

import (
	"bytes"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func init() {
	log.SetLevel(log.WarnLevel)
}

func expectLogMessage(t *testing.T, message string, fn func()) {
	expectLogMessages(t, []string{message}, fn)
}

func expectLogMessages(t *testing.T, messages []string, fn func()) {
	originalLevel := log.GetLevel()
	log.SetLevel(log.DebugLevel)
	logBuffer := redirectLogs(t, fn)

	for _, message := range messages {
		assert.Contains(t, logBuffer.String(), message)
	}
	log.SetLevel(originalLevel)
}

func redirectLogs(t *testing.T, fn func()) (buffer bytes.Buffer) {
	log.SetOutput(&buffer)
	defer log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
	defer log.SetLevel(log.WarnLevel)

	fn()

	t.Log(buffer.String())
	return
}
