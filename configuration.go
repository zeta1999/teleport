package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	log "github.com/sirupsen/logrus"
)

var (
	configDirectory                = "./config/"
	apisConfigDirectory            = "./sources/apis/"
	databasesConfigDirectory       = "./sources/databases/"
	legacyApisConfigDirectory      = "./apis/"
	legacyDatabasesConfigDirectory = "./databases/"

	transformsConfigDirectory = "./transforms/"

	// SQLTransforms is a list of configured SQL statements for updateTransforms to use
	SQLTransforms = make(map[string]string)
)

func workingDir() (path string) {
	path, ok := os.LookupEnv("PADPATH")
	if ok {
		return
	}

	path, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	return
}

func readFiles(directory string) (files []os.FileInfo, err error) {
	items, err := ioutil.ReadDir(filepath.Join(workingDir(), directory))
	if err != nil {
		return make([]os.FileInfo, 0), err
	}

	for _, fileinfo := range items {
		if fileinfo.IsDir() {
			continue
		} else if strings.HasPrefix(fileinfo.Name(), ".") {
			continue
		}

		files = append(files, fileinfo)
	}

	return
}

func fileNameWithoutExtension(filename string) string {
	extension := filepath.Ext(filename)

	return filename[0 : len(filename)-len(extension)]
}

func validateIn(v interface{}, param string) error {
	if v == nil || v == "" {
		return nil
	}

	st := reflect.ValueOf(v)
	if st.Kind() != reflect.String {
		return errors.New("in only validates strings")
	}

	for _, a := range strings.Split(param, "|") {
		if strings.ToLower(a) == strings.ToLower(v.(string)) {
			return nil
		}
	}

	return fmt.Errorf("value '%s' not allowed. Allowed values: %s", v.(string), param)
}
