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
	URL     string
	Options map[string]string
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

func aboutDB(source string) {
	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	fmt.Println("Name: ", source)
	if strings.HasPrefix(Connections[source].Config.URL, "redshift://") {
		fmt.Println("Type: ", "Redshift")
	} else {
		switch driverType := fmt.Sprintf("%T", database.Driver()); driverType {
		case "*pq.Driver":
			fmt.Println("Type: ", "Postgres")
		case "*sqlite3.SQLiteDriver":
			fmt.Println("Type: ", "SQLite")
		default:
			fmt.Println("Type: ", "MySQL")
		}
	}
}
