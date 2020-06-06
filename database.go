package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/xo/dburl"
)

var (
	dbs = make(map[string]*sql.DB)

	fullStrategyOpts = map[string]string{}
)

func extractLoadDatabase(source string, destination string, tableName string, strategy string, strategyOpts map[string]string) {
	log.Printf("Starting extract-load from *%s* to *%s* with table `%s`", source, destination, tableName)

	var sourceTable Table
	var destinationTable Table
	var columns []Column
	var csvfile string

	destinationTableName := fmt.Sprintf("%s_%s", source, tableName)

	steps := []func() error{
		func() error { return connectDatabaseWithLogging(source) },
		func() error { return connectDatabaseWithLogging(destination) },
		func() error { return inspectTable(source, tableName, &sourceTable) },
		func() error {
			return createDestinationTableIfNotExists(destination, destinationTableName, &sourceTable, &destinationTable)
		},
		func() error {
			return extractSource(&sourceTable, &destinationTable, strategy, strategyOpts, &columns, &csvfile)
		},
		func() error { return createStagingTable(&destinationTable) },
		func() error { return loadDestination(&destinationTable, &columns, &csvfile) },
		func() error { return promoteStagingTable(&destinationTable) },
	}

	for _, step := range steps {
		err := step()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func extractDatabase(source string, tableName string) {
	log.Printf("Starting extract from *%s* table `%s` to CSV", source, tableName)

	var table Table
	var csvfile string

	steps := []func() error{
		func() error { return connectDatabaseWithLogging(source) },
		func() error { return inspectTable(source, tableName, &table) },
		func() error { return extractSource(&table, nil, "full", fullStrategyOpts, nil, &csvfile) },
	}

	for _, step := range steps {
		err := step()
		if err != nil {
			log.Fatalf("ERROR: %s", err)
		}
	}

	log.Printf("Extracted to: %s\n", csvfile)
}

func connectDatabaseWithLogging(source string) (err error) {
	log.Printf("Connecting to database: *%s*", source)

	_, err = connectDatabase(source)

	return
}

func inspectTable(source string, tableName string, table *Table) (err error) {
	log.Printf("Describing table `%s` in *%s*...", tableName, source)

	dumpedTable, err := dumpTableMetadata(source, tableName)
	if err != nil {
		return
	}

	*table = *dumpedTable
	return
}

func extractSource(sourceTable *Table, destinationTable *Table, strategy string, strategyOpts map[string]string, columns *[]Column, csvfile *string) (err error) {
	log.Printf("Exporting CSV of table `%s` from *%s*", sourceTable.Table, sourceTable.Source)

	var exportColumns []Column

	if destinationTable != nil {
		exportColumns = importableColumns(destinationTable, sourceTable)
	} else {
		exportColumns = sourceTable.Columns
	}

	var whereStatement string
	switch strategy {
	case "full":
		whereStatement = ""
	case "incremental":
		hoursAgo, err := strconv.Atoi(strategyOpts["hours_ago"])
		if err != nil {
			return fmt.Errorf("invalid value `%s` for hours-ago", strategyOpts["hours_ago"])
		}
		updateTime := (time.Now().Add(time.Duration(-1*hoursAgo) * time.Hour)).Format("2006-01-02 15:04:05")
		whereStatement = fmt.Sprintf("%s > '%s'", strategyOpts["modified_at_column"], updateTime)
	}

	file, err := exportCSV(sourceTable.Source, sourceTable.Table, exportColumns, whereStatement)
	if err != nil {
		return err
	}

	*csvfile = file
	if columns != nil {
		*columns = exportColumns
	}
	return
}

func exportCSV(source string, table string, columns []Column, whereStatement string) (string, error) {
	database, err := connectDatabase(source)
	if err != nil {
		return "", fmt.Errorf("Database Open Error: %w", err)
	}

	exists, err := tableExists(source, table)
	if err != nil {
		return "", err
	} else if !exists {
		return "", fmt.Errorf("table \"%s\" not found in \"%s\"", table, source)
	}

	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
	}

	tmpfile, err := ioutil.TempFile("/tmp/", fmt.Sprintf("extract-%s-%s", table, source))
	if err != nil {
		return "", err
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columnNames, ", "), table)
	if whereStatement != "" {
		query += fmt.Sprintf(" WHERE %s", whereStatement)
	}

	rows, err := database.Query(query)
	if err != nil {
		return "", err
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
			return "", err
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
			return "", err
		}
	}

	writer.Flush()

	if err := tmpfile.Close(); err != nil {
		return "", err
	}

	return tmpfile.Name(), nil
}

func connectDatabase(source string) (*sql.DB, error) {
	if dbs[source] != nil {
		return dbs[source], nil
	}

	url := Connections[source].Config.URL
	database, err := dburl.Open(url)
	if err != nil {
		return nil, err
	}

	err = database.Ping()
	if err != nil {
		return nil, err
	}

	dbs[source] = database
	return dbs[source], nil
}
