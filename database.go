package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"github.com/hundredwatt/teleport/schema"
	log "github.com/sirupsen/logrus"
	"github.com/xo/dburl"
)

var (
	dbs = make(map[string]*sql.DB)

	fullStrategyOpts = StrategyOptions{"full", "", "", ""}
)

func extractLoadDatabase(source string, destination string, tableName string, strategyOpts StrategyOptions) {
	fnlog := log.WithFields(log.Fields{
		"from":     source,
		"to":       destination,
		"table":    tableName,
		"strategy": strategyOpts.Strategy,
	})
	fnlog.Info("Starting extract-load")

	var sourceTable schema.Table
	var destinationTable schema.Table
	var columns []schema.Column
	var csvfile string

	destinationTableName := fmt.Sprintf("%s_%s", source, tableName)

	RunWorkflow([]func() error{
		func() error { return connectDatabaseWithLogging(source) },
		func() error { return connectDatabaseWithLogging(destination) },
		func() error { return inspectTable(source, tableName, &sourceTable) },
		func() error {
			return createDestinationTableIfNotExists(destination, destinationTableName, &sourceTable, &destinationTable)
		},
		func() error {
			return extractSource(&sourceTable, &destinationTable, strategyOpts, &columns, &csvfile)
		},
		func() error { return load(&destinationTable, &columns, &csvfile, strategyOpts) },
	}, func() {
		fnlog.WithField("rows", currentWorkflow.RowCounter).Info("Completed extract-load 🎉")
	})
}

func extractDatabase(source string, tableName string) {
	log.WithFields(log.Fields{
		"from":  source,
		"table": tableName,
	}).Info("Extracting table data to CSV")

	var table schema.Table
	var csvfile string

	RunWorkflow([]func() error{
		func() error { return connectDatabaseWithLogging(source) },
		func() error { return inspectTable(source, tableName, &table) },
		func() error { return extractSource(&table, nil, fullStrategyOpts, nil, &csvfile) },
	}, func() {
		log.WithFields(log.Fields{
			"file": csvfile,
			"rows": currentWorkflow.RowCounter,
		}).Info("Extract to CSV completed 🎉")
	})
}

func connectDatabaseWithLogging(source string) (err error) {
	log.WithFields(log.Fields{
		"database": source,
	}).Debug("Establish connection to Database")

	_, err = connectDatabase(source)

	return
}

func inspectTable(source string, tableName string, table *schema.Table) (err error) {
	log.WithFields(log.Fields{
		"database": source,
		"table":    tableName,
	}).Debug("Inspecting Table")

	db, _ := connectDatabase(source)

	dumpedTable, err := schema.DumpTableMetadata(db, tableName)
	if err != nil {
		return
	}
	dumpedTable.Source = source

	*table = *dumpedTable
	return
}

func extractSource(sourceTable *schema.Table, destinationTable *schema.Table, strategyOpts StrategyOptions, columns *[]schema.Column, csvfile *string) (err error) {
	log.WithFields(log.Fields{
		"database": sourceTable.Source,
		"table":    sourceTable.Name,
		"type":     strategyOpts.Strategy,
	}).Debug("Exporting CSV of table data")

	var exportColumns []schema.Column

	if destinationTable != nil {
		exportColumns = importableColumns(destinationTable, sourceTable)
	} else {
		exportColumns = sourceTable.Columns
	}

	var whereStatement string
	switch strategyOpts.Strategy {
	case "full":
		whereStatement = ""
	case "modified-only":
		hoursAgo, err := strconv.Atoi(strategyOpts.HoursAgo)
		if err != nil {
			return fmt.Errorf("invalid value `%s` for hours-ago", strategyOpts.HoursAgo)
		}
		updateTime := (time.Now().Add(time.Duration(-1*hoursAgo) * time.Hour)).Format("2006-01-02 15:04:05")
		whereStatement = fmt.Sprintf("%s > '%s'", strategyOpts.ModifiedAtColumn, updateTime)
	}

	file, err := exportCSV(sourceTable.Source, sourceTable.Name, exportColumns, whereStatement)
	if err != nil {
		return err
	}

	*csvfile = file
	if columns != nil {
		*columns = exportColumns
	}
	return
}

func exportCSV(source string, table string, columns []schema.Column, whereStatement string) (string, error) {
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

		IncrementRowCounter()

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
			case bool:
				writeBuffer[i] = strconv.FormatBool(rawResult[i].(bool))
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

		if Preview && GetRowCounter() >= int64(PreviewLimit) {
			break
		}
	}

	writer.Flush()

	if err := tmpfile.Close(); err != nil {
		return "", err
	}

	if Preview {
		content, err := ioutil.ReadFile(tmpfile.Name())
		if err != nil {
			return "", err
		}

		log.WithFields(log.Fields{
			"limit": PreviewLimit,
			"file":  tmpfile.Name(),
		}).Debug("Results CSV Generated")

		log.Debug(fmt.Sprintf(`CSV Contents:
	Headers:
	%s

	Body:
%s
				`, strings.Join(columnNames, ","), indentString(string(content))))
	}

	return tmpfile.Name(), nil
}

func connectDatabase(source string) (*sql.DB, error) {
	if dbs[source] != nil {
		return dbs[source], nil
	}

	url := Databases[source].URL
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
