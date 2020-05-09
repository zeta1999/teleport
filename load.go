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

	if !tableExists(destination, destinationTableName) {
		createTable(destinationDatabase, destinationTableName, table)
	} else {
		// 		destinationTable, err := dumpTableMetadata(destination, destinationTableName)
		// 		if err != nil {
		// 			log.Fatal("Table Metadata Error:", err)
		// }

		// 		destinationTable.compatabilityWith(table)
	}

	file, err := exportCSV(source, tableName)
	if err != nil {
		log.Fatal("Extract Error:", err)
	}

	importCSV(destination, destinationTableName, file)
}
