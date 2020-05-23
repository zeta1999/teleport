package main

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
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
	Results          *[]dataObject
}

func extractLoadDatabase(source string, destination string, tableName string, strategy string, strategyOpts map[string]string) {
	log.Printf("Starting extract-load from *%s* to *%s* with table `%s`", source, destination, tableName)

	task := taskContext{source, destination, tableName, strategy, strategyOpts, nil, nil, "", nil, nil}

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

func exportCSV(source string, table string, columns []Column, whereStatement string) (string, error) {
	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	if !tableExists(source, table) {
		log.Fatalf("table \"%s\" not found in \"%s\"", table, source)
	}

	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
	}

	tmpfile, err := ioutil.TempFile("/tmp/", fmt.Sprintf("extract-%s-%s", table, source))
	if err != nil {
		log.Fatal(err)
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columnNames, ", "), table)
	if whereStatement != "" {
		query += fmt.Sprintf(" WHERE %s", whereStatement)
	}

	rows, err := database.Query(query)
	if err != nil {
		log.Fatal(err)
	}

	writer := csv.NewWriter(tmpfile)
	destination := make([]interface{}, len(columnNames))
	rawResult := make([]interface{}, len(columnNames))
	writeBuffer := make([]string, len(columnNames))
	for i := range rawResult {
		destination[i] = &rawResult[i]
	}
	for rows.Next() {
		err := rows.Scan(destination...)
		if err != nil {
			log.Fatal(err)
		}

		for i := range columns {
			switch rawResult[i].(type) {
			case time.Time:
				writeBuffer[i] = rawResult[i].(time.Time).Format("2006-01-02 15:04:05")
			case int64:
				writeBuffer[i] = strconv.FormatInt(rawResult[i].(int64), 10)
			case string:
				writeBuffer[i] = rawResult[i].(string)
			case float64:
				writeBuffer[i] = strconv.FormatFloat(rawResult[i].(float64), 'E', -1, 64)
			case nil:
				writeBuffer[i] = ""
			default:
				writeBuffer[i] = string(rawResult[i].([]byte))
			}
		}

		err = writer.Write(writeBuffer)
		if err != nil {
			log.Fatal(err)
		}
	}

	writer.Flush()

	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	return tmpfile.Name(), nil
}

func extractDatabase(source string, table string) {
	tableDefinition, err := dumpTableMetadata(source, table)
	if err != nil {
		log.Fatal("Dump Table Metadata Error:", err)
	}

	tmpfile, err := exportCSV(source, table, tableDefinition.Columns, "")
	if err != nil {
		log.Fatal("Export CSV error:", err)
	}

	log.Printf("Extracted to: %s\n", tmpfile)
}
