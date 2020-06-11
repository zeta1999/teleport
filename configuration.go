package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/ilyakaznacheev/cleanenv"
	log "github.com/sirupsen/logrus"
)

var (
	apisConfigDirectory          = "./apis/"
	databasesConfigDirectory     = "./databases/"
	transformsConfigDirectory    = "./transforms/"
	apiTransformsConfigDirectory = "./apis/parsers/"

	// Databases contains the configuration for all databases
	Databases = make(map[string]Database)

	// APIs contains the configuration for all APIs
	APIs = make(map[string]API)

	// APITransforms is a list of configured Starlark Transforms for endpoints to use
	APITransforms = make(map[string]string)

	// SQLTransforms is a list of configured SQL statements for updateTransforms to use
	SQLTransforms = make(map[string]string)
)

type Database struct {
	URL      string
	Options  map[string]string
	Readonly bool
}

type API struct {
	BaseURL     string `yaml:"base_url"`
	Headers     map[string]string
	QueryString map[string]string `yaml:"query_string"`
	// TODO: allow inheritance for the below 3 attributes
	// ResponseType   string `json:"response_type"`
	// PaginationType string `json:"pagination_type"`
	// MaxPages       int    `json:"max_pages"`
	Endpoints map[string]Endpoint
}

type Endpoint struct {
	URL            string
	Method         string
	Headers        map[string]string
	QueryString    map[string]string `yaml:"query_string"`
	ResponseType   string            `json:"response_type" yaml:"response_type"`
	PaginationType string            `json:"pagination_type"`
	MaxPages       int               `json:"max_pages"`
	Transforms     []string          `yaml:"parsers"`
}

func readConfiguration() {
	// Databases
	for _, fileinfo := range readFiles(databasesConfigDirectory) {
		var database Database
		err := cleanenv.ReadConfig(filepath.Join(workingDir(), databasesConfigDirectory, fileinfo.Name()), &database)
		if err != nil {
			log.Fatal(err)
		}
		database.URL = os.ExpandEnv(database.URL)

		Databases[fileNameWithoutExtension(fileinfo.Name())] = database
	}

	// APIs
	for _, fileinfo := range readFiles(apisConfigDirectory) {
		var api API
		err := cleanenv.ReadConfig(filepath.Join(workingDir(), apisConfigDirectory, fileinfo.Name()), &api)
		if err != nil {
			log.Fatal(err)
		}

		APIs[fileNameWithoutExtension(fileinfo.Name())] = api
	}
}

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

func readFiles(directory string) (files []os.FileInfo) {
	items, err := ioutil.ReadDir(filepath.Join(workingDir(), directory))
	if err != nil {
		log.Fatal(err)
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
