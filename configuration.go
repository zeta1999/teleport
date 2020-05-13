package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
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
	fmt.Println("Name: ", source)
	fmt.Printf("Type: %s\n", DbDialect(Connections[source]).HumanName)
}

func dbTerminal(source string) {
	command := DbDialect(Connections[source]).TerminalCommand
	if command == "" {
		log.Fatalf("Not implemented for this database type")
	}

	binary, err := exec.LookPath(command)
	if err != nil {
		log.Fatalf("command exec err (%s): %s", command, err)
	}

	env := os.Environ()

	err = syscall.Exec(binary, []string{command, Connections[source].Config.URL}, env)
	if err != nil {
		log.Fatalf("Syscall error: %s", err)
	}

}
