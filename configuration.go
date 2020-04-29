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

	// Connections is a list of configuration source connections
	Connections = make(map[string]Connection)
)

type Connection struct {
	Name   string
	Config Configuration
}

type Configuration struct {
	Url string
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
		if err != nil {
			log.Fatal(errDecode)
		}

		Connections[connection.Name] = connection
	}
}
