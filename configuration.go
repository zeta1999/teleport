package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

var (
	connectionsConfigDirectory = "./config/connections/"
	endpointsConfigDirectory   = "./config/endpoints/"

	// Connections is a list of configuration source connections
	Connections = make(map[string]Connection)

	// Endpoints is a list of configured HTTP endpoints
	Endpoints = make(map[string]Endpoint)

	// Transforms is a list of configured Starlark Transforms for endpoints to use
	Transforms = make(map[string]string)
)

type Connection struct {
	Name   string
	Config Configuration
}

type Configuration struct {
	URL     string
	Options map[string]string
}

type Endpoint struct {
	Name           string
	Method         string
	URL            string
	Headers        map[string]string
	ResponseType   string `json:"response_type"`
	PaginationType string `json:"pagination_type"`
	MaxPages       int    `json:"max_pages"`
	Transforms     []string
}

func readConnections() {
	files, err := ioutil.ReadDir(connectionsConfigDirectory)
	if err != nil {
		log.Fatal(err)
	}
	for _, fileinfo := range files {
		file, err := os.Open(fmt.Sprintf("%s%s", connectionsConfigDirectory, fileinfo.Name()))
		if err != nil {
			log.Fatal(err)
		}

		defer file.Close()
		decoder := json.NewDecoder(file)
		connection := Connection{strings.Replace(fileinfo.Name(), ".json", "", 1), Configuration{}}
		errDecode := decoder.Decode(&connection.Config)
		if errDecode != nil {
			log.Fatalf("error reading config file `%s`: %s", fileinfo.Name(), errDecode)
		}

		Connections[connection.Name] = connection
	}
}

func readEndpoints() {
	files, err := ioutil.ReadDir(endpointsConfigDirectory)
	if err != nil {
		log.Fatal(err)
	}
	for _, fileinfo := range files {
		if !strings.HasSuffix(fileinfo.Name(), ".json") {
			continue
		}

		file, err := os.Open(fmt.Sprintf("%s%s", endpointsConfigDirectory, fileinfo.Name()))
		if err != nil {
			log.Fatal(err)
		}

		defer file.Close()
		decoder := json.NewDecoder(file)
		endpoint := Endpoint{strings.Replace(fileinfo.Name(), ".json", "", 1), "", "", make(map[string]string, 0), "", "", -1, make([]string, 0)}
		errDecode := decoder.Decode(&endpoint)
		if errDecode != nil {
			log.Fatalf("error reading config file `%s`: %s", fileinfo.Name(), errDecode)
		}

		Endpoints[endpoint.Name] = endpoint
	}
}
