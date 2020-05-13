package main

import (
	"fmt"
	"log"
	"reflect"
	"runtime"
)

type taskContext struct {
	Source           string
	Destination      string
	TableName        string
	SourceTable      *Table
	DestinationTable *Table
	CSVFile          string
}

func load(source string, destination string, tableName string) {
	log.Printf("Starting extract-load from *%s* to *%s* with table `%s`", source, destination, tableName)

	task := taskContext{source, destination, tableName, nil, nil, ""}

	steps := []func(tc *taskContext) error{
		connectSourceDatabase,
		connectDestinationDatabase,
		createDestinationTableIfNotExists,
		inspectDestinationTableIfNotCreated,
		extractSource,
		loadDestination,
	}

	for _, step := range steps {
		err := step(&task)
		if err != nil {
			log.Fatalf("Error in %s: %s", getFunctionName(step), err)
		}
	}
}

func (tc *taskContext) destinationTableName() string {
	return fmt.Sprintf("%s_%s", tc.Source, tc.TableName)
}

func connectSourceDatabase(tc *taskContext) error {
	log.Printf("Connecting to *%s*...", tc.Source)
	_, err := connectDatabase(tc.Source)
	if err != nil {
		return err
	}

	table, err := dumpTableMetadata(tc.Source, tc.TableName)
	if err != nil {
		return err
	}

	tc.SourceTable = table

	return nil
}

func connectDestinationDatabase(tc *taskContext) error {
	log.Printf("Connecting to *%s*...", tc.Destination)
	_, err := connectDatabase(tc.Source)
	if err != nil {
		return err
	}
	return nil
}

func createDestinationTableIfNotExists(tc *taskContext) error {
	if tableExists(tc.Destination, tc.destinationTableName()) {
		return nil
	}

	log.Printf("Table `%s` does not exist in *%s*, creating", tc.TableName, tc.Destination)

	tc.DestinationTable = &Table{tc.Destination, tc.destinationTableName(), make([]Column, len(tc.SourceTable.Columns))}
	copy(tc.DestinationTable.Columns, tc.SourceTable.Columns)

	return createTable(dbs[tc.Destination], tc.destinationTableName(), tc.SourceTable)
}

func inspectDestinationTableIfNotCreated(tc *taskContext) error {
	if tc.DestinationTable != nil {
		return nil
	}

	log.Printf("Inspecting table `%s` in *%s*", tc.destinationTableName(), tc.Destination)
	table, err := dumpTableMetadata(tc.Destination, tc.destinationTableName())
	if err != nil {
		return nil
	}

	tc.DestinationTable = table
	return nil
}

func extractSource(tc *taskContext) error {
	log.Printf("Exporting CSV of table `%s` from *%s*", tc.TableName, tc.Source)
	exportColumns := importableColumns(tc.DestinationTable, tc.SourceTable)

	file, err := exportCSV(tc.Source, tc.TableName, exportColumns)

	tc.CSVFile = file
	return err
}

func loadDestination(tc *taskContext) error {
	log.Printf("Importing CSV into table `%s` in *%s*", tc.destinationTableName(), tc.Destination)
	importCSV(tc.Destination, tc.destinationTableName(), tc.CSVFile)
	return nil
}

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}
