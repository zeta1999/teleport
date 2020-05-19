package main

import (
	"fmt"
	"log"
	"reflect"
	"runtime"
	"strconv"
	"time"
)

type taskContext struct {
	Source           string
	Destination      string
	TableName        string
	Strategy         string
	StrategyOpts     map[string]string
	SourceTable      *Table
	DestinationTable *Table
	CSVFile          string
	Columns          *[]Column
}

func load(source string, destination string, tableName string, strategy string, strategyOpts map[string]string) {
	log.Printf("Starting extract-load from *%s* to *%s* with table `%s`", source, destination, tableName)

	task := taskContext{source, destination, tableName, strategy, strategyOpts, nil, nil, "", nil}

	steps := []func(tc *taskContext) error{
		connectSourceDatabase,
		connectDestinationDatabase,
		createDestinationTableIfNotExists,
		inspectDestinationTableIfNotCreated,
		extractSource,
		createStagingTable,
		loadDestination,
		promoteStagingTable,
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
	_, err := connectDatabase(tc.Destination)
	if err != nil {
		return err
	}
	return nil
}

func createDestinationTableIfNotExists(tc *taskContext) error {
	if tableExists(tc.Destination, tc.destinationTableName()) {
		return nil
	}

	log.Printf("Table `%s` does not exist in *%s*, creating", tc.destinationTableName(), tc.Destination)

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
	var whereStatement string
	switch tc.Strategy {
	case "full":
		whereStatement = ""
	case "incremental":
		hoursAgo, err := strconv.Atoi(tc.StrategyOpts["hours_ago"])
		if err != nil {
			log.Fatal("invalid value for hours-ago")
		}
		updateTime := (time.Now().Add(time.Duration(-1*hoursAgo) * time.Hour)).Format("2006-01-02 15:04:05")
		whereStatement = fmt.Sprintf("%s > '%s'", tc.StrategyOpts["modified_at_column"], updateTime)
	}

	file, err := exportCSV(tc.Source, tc.TableName, exportColumns, whereStatement)

	tc.CSVFile = file
	tc.Columns = &exportColumns
	return err
}

func createStagingTable(tc *taskContext) error {
	log.Printf("Creating staging table `staging_%s` in *%s*", tc.destinationTableName(), tc.Destination)

	_, err := dbs[tc.Destination].Exec(fmt.Sprintf(DbDialect(Connections[tc.Destination]).CreateStagingTableQuery, tc.destinationTableName()))

	return err
}

func loadDestination(tc *taskContext) error {
	log.Printf("Importing CSV into table `staging_%s` in *%s*", tc.destinationTableName(), tc.Destination)
	importCSV(tc.Destination, fmt.Sprintf("staging_%s", tc.destinationTableName()), tc.CSVFile, *tc.Columns)
	return nil
}

func promoteStagingTable(tc *taskContext) error {
	log.Printf("Promote staging table `staging_%[1]s` to primary `%[1]s` in *%[2]s*", tc.destinationTableName(), tc.Destination)

	_, err := dbs[tc.Destination].Exec(fmt.Sprintf(DbDialect(Connections[tc.Destination]).PromoteStagingTableQuery, tc.destinationTableName()))
	if err != nil {
		return err
	}

	return nil
}

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}
