package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/dnaeon/go-vcr/cassette"
	"github.com/dnaeon/go-vcr/recorder"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
)

var Recorder *recorder.Recorder

func testAPI(endpointName string) {
	var endpoint Endpoint
	err := readEndpointConfiguration(endpointName, &endpoint)
	if err != nil {
		log.Fatal(err)
	}

	destinationTableName := strings.TrimSuffix(filepath.Base(endpointName), filepath.Ext(endpointName))

	cassetteDir := filepath.Join(os.Getenv("PADPATH"), "testing", "api_recordings")
	err = os.MkdirAll(cassetteDir, 0755)
	if err != nil {
		log.Fatal(err)
	}

	cassettePath := filepath.Join(cassetteDir, destinationTableName, "cassette")
	var mode recorder.Mode
	if _, err = os.Stat(fmt.Sprintf("%s.yaml", cassettePath)); err == nil {
		mode = recorder.ModeReplaying
	} else {
		mode = recorder.ModeRecording
	}
	r, err := recorder.NewAsMode(cassettePath, mode, http.DefaultTransport)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Stop()
	r.SetMatcher(func(r *http.Request, i cassette.Request) bool {
		if maskedURLMatch(i.URL, r.URL.String()) {
			return true
		}
		return cassette.DefaultMatcher(r, i)
	})

	r.AddFilter(func(i *cassette.Interaction) error {
		i.Request.URL = os.Expand(endpoint.unexpandedURL, func(key string) string {
			return strings.Repeat("*", len(os.Getenv(key)))
		})

		delete(i.Request.Headers, "Authorization")

		if i.Response.Body != "" {
			newBody, err := anonymizeBody(endpoint.ResponseType, i.Response.Body)
			if err != nil {
				return err
			}
			i.Response.Body = newBody
		}
		return nil
	})

	hook := new(testingHook)
	log.AddHook(hook)

	Recorder = r
	Preview = true
	extractAPI(endpointName)

	r.Stop()
	if hook.csvfile != "" {
		err = copyFile(hook.csvfile, filepath.Join(cassetteDir, destinationTableName, "results.csv"))
		if err != nil {
			log.Fatal(err)
		}
	}
}

type testingHook struct {
	csvfile string
}

func (hook *testingHook) Fire(entry *log.Entry) error {
	if entry.Data["file"] != nil {
		hook.csvfile = entry.Data["file"].(string)
	}
	return nil
}

func (hook *testingHook) Levels() []log.Level {
	return []log.Level{
		log.InfoLevel,
	}
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return err
	}
	return dstFile.Close()
}

var dict = map[string]string{
	"Santana": "S******",
}

func anonymizeBody(responseType string, body string) (string, error) {
	r := ioutil.NopCloser(bytes.NewReader([]byte(body)))
	unmarshalled, err := unmarshalBody(responseType, r)
	if err != nil {
		return "", err
	}

	original := reflect.ValueOf(unmarshalled)
	copy := reflect.New(original.Type()).Elem()
	anonymizeRecursive(copy, original)

	if responseType == "json" {
		marshalled, err := jsoniter.Marshal(copy.Interface())
		if err != nil {
			return "", err
		}

		return string(marshalled), nil
	} else if responseType == "csv" {

		buf := new(bytes.Buffer)
		writer := csv.NewWriter(buf)
		writer.Write(original.Interface().(table)[0])
		writer.WriteAll(copy.Interface().(table)[1:])
		return buf.String(), nil
	}

	return "Error", nil
}

// adapted from https://gist.github.com/hvoecking/10772475
func anonymizeRecursive(copy, original reflect.Value) {
	switch original.Kind() {
	// The first cases handle nested structures and translate them recursively

	// If it is a pointer we need to unwrap and call once again
	case reflect.Ptr:
		// To get the actual value of the original we have to call Elem()
		// At the same time this unwraps the pointer so we don't end up in
		// an infinite recursion
		originalValue := original.Elem()
		// Check if the pointer is nil
		if !originalValue.IsValid() {
			return
		}
		// Allocate a new object and set the pointer to it
		copy.Set(reflect.New(originalValue.Type()))
		// Unwrap the newly created pointer
		anonymizeRecursive(copy.Elem(), originalValue)

	// If it is an interface (which is very similar to a pointer), do basically the
	// same as for the pointer. Though a pointer is not the same as an interface so
	// note that we have to call Elem() after creating a new object because otherwise
	// we would end up with an actual pointer
	case reflect.Interface:
		// Get rid of the wrapping interface
		originalValue := original.Elem()
		// Create a new object. Now new gives us a pointer, but we want the value it
		// points to, so we have to call Elem() to unwrap it
		copyValue := reflect.New(originalValue.Type()).Elem()
		anonymizeRecursive(copyValue, originalValue)
		copy.Set(copyValue)

	// If it is a struct we translate each field
	case reflect.Struct:
		for i := 0; i < original.NumField(); i += 1 {
			anonymizeRecursive(copy.Field(i), original.Field(i))
		}

	// If it is a slice we create a new slice and translate each element
	case reflect.Slice:
		copy.Set(reflect.MakeSlice(original.Type(), original.Len(), original.Cap()))
		for i := 0; i < original.Len(); i += 1 {
			anonymizeRecursive(copy.Index(i), original.Index(i))
		}

	// If it is a map we create a new map and translate each value
	case reflect.Map:
		copy.Set(reflect.MakeMap(original.Type()))
		for _, key := range original.MapKeys() {
			originalValue := original.MapIndex(key)
			// New gives us a pointer, but again we want the value
			copyValue := reflect.New(originalValue.Type()).Elem()
			anonymizeRecursive(copyValue, originalValue)
			copy.SetMapIndex(key, copyValue)
		}

	// Otherwise we cannot traverse anywhere so this finishes the the recursion

	// If it is a string translate it (yay finally we're doing what we came for)
	case reflect.String:
		valueString := original.Interface().(string)
		if _, err := strconv.ParseInt(valueString, 10, 64); err == nil {
			copy.SetString(valueString)
		} else if valueString == "" {
			copy.SetString(valueString)
		} else if valueString[0] >= '0' && valueString[0] <= '9' {
			copy.SetString(valueString)
		} else {
			copy.SetString(anonymize(valueString))
		}

	// And everything else will simply be taken from the original
	default:
		copy.Set(original)
	}
}

func anonymize(value string) string {
	words := strings.Split(value, " ")
	for i := range words {
		words[i] = string(words[i][0]) + strings.Repeat("*", len(words[i])-1)
	}

	if len(words) > 6 {
		return strings.Join(words[0:5], " ") + "..."
	} else {
		return strings.Join(words, " ")
	}
}

func maskedURLMatch(url1, url2 string) bool {
	if len(url1) != len(url2) {
		return false
	}

	for j := range url1 {
		if url1[j] == '*' || url2[j] == '*' {
			continue
		}

		if url1[j] != url2[j] {
			return false
		}
	}

	return true
}
