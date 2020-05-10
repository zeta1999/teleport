package main

import (
	"fmt"
	"log"
)

func load(source string, destination string, tableName string) {
	destinationTableName := fmt.Sprintf("%s_%s", source, tableName)

	log.Printf("Starting extract-load from *%s* to *%s* with table `%s`", source, destination, tableName)

	log.Printf("Connecting to *%s*...", source)
	destinationDatabase, err := connectDatabase(destination)
	if err != nil {
		log.Fatal("Connect Database Error:", err)
	}

	log.Printf("Inspecting table `%s` in *%s*", tableName, source)
	table, err := dumpTableMetadata(source, tableName)
	if err != nil {
		log.Fatal("Table Metadata Error:", err)
	}

	var exportColumns []Column
	if !tableExists(destination, destinationTableName) {
		log.Printf("Table `%s` does not exist in *%s*, creating", tableName, destination)
		createTable(destinationDatabase, destinationTableName, table)
		exportColumns = table.Columns
	} else {
		log.Printf("Inspecting table `%s` in *%s*", destinationTableName, destination)
		destinationTable, err := dumpTableMetadata(destination, destinationTableName)
		if err != nil {
			log.Fatal("Table Metadata Error:", err)
		}

		exportColumns = importableColumns(destinationTable, table)
	}

	log.Printf("Exporting CSV of table `%s` from *%s*", tableName, source)
	file, err := exportCSV(source, tableName, exportColumns)
	if err != nil {
		log.Fatal("Extract Error:", err)
	}

	log.Printf("Importing CSV into table `%s` in *%s*", destinationTableName, destination)
	importCSV(destination, destinationTableName, file)
}
