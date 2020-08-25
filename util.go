package main

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const charset = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func indentString(s string) string {
	return "\t" + strings.Join(strings.Split(s, "\n"), "\n\t")
}

func randomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func generateCSV(headers []string, name string, fn func(*csv.Writer) error) (string, error) {
	tmpfile, err := ioutil.TempFile("/tmp/", name)
	if err != nil {
		return "", err
	}

	writer := csv.NewWriter(&WorkflowWriter{Writer: tmpfile})

	err = fn(writer)
	if err != nil {
		return "", err
	}

	writer.Flush()

	if err := tmpfile.Close(); err != nil {
		return "", err
	}

	if Preview {
		err = printCSVPreview(headers, tmpfile.Name())
		if err != nil {
			return "", err
		}
	}

	return tmpfile.Name(), nil
}

func formatForCSV(value interface{}) string {
	switch value.(type) {
	case nil:
		return ""
	case string:
		return string(value.(string))
	case bool:
		return strconv.FormatBool(value.(bool))
	case int:
		return strconv.Itoa(value.(int))
	case int64:
		return strconv.FormatInt(value.(int64), 10)
	case float64:
		return strconv.FormatFloat(value.(float64), 'E', -1, 64)
	case time.Time:
		return value.(time.Time).Format(time.RFC3339)
	case map[string]interface{}:
		return ""
	case []interface{}:
		return ""
	default:
		return string(value.([]byte))
	}
}

func printCSVPreview(headers []string, file string) error {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"limit": PreviewLimit,
		"file":  file,
	}).Debug("Results CSV Generated")

	log.Debug(fmt.Sprintf(`CSV Contents:
Headers:
%s

Body:
%s
		`, strings.Join(headers, ","), indentString(string(content))))

	return nil
}

func byteCountDecimal(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}
