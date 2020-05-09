package main

import (
	"fmt"
	"log"
)

func load(source string, destination string, tableName string) {
	destinationTableName := fmt.Sprintf("%s_%s", source, tableName)

	destinationDatabase, err := connectDatabase(destination)
	if err != nil {
		log.Fatal("Connect Database Error:", err)
	}

	table, err := dumpTableMetadata(source, tableName)
	if err != nil {
		log.Fatal("Table Metadata Error:", err)
	}

	var exportColumns []Column
	if !tableExists(destination, destinationTableName) {
		createTable(destinationDatabase, destinationTableName, table)
		exportColumns = table.Columns
	} else {
		destinationTable, err := dumpTableMetadata(destination, destinationTableName)
		if err != nil {
			log.Fatal("Table Metadata Error:", err)
		}

		exportColumns = importableColumns(destinationTable, table)
	}

	file, err := exportCSV(source, tableName, exportColumns)
	if err != nil {
		log.Fatal("Extract Error:", err)
	}

	importCSV(destination, destinationTableName, file)
}
