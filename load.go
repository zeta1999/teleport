package main

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/hundredwatt/teleport/schema"
	log "github.com/sirupsen/logrus"
)

type LoadStrategy string

const (
	Full         LoadStrategy = "Full"
	Incremental  LoadStrategy = "Incremental"
	ModifiedOnly LoadStrategy = "ModifiedOnly"
)

type LoadOptions struct {
	Strategy         LoadStrategy
	PrimaryKey       string
	ModifiedAtColumn string
	GoBackHours      int
}

func load(destinationTable *schema.Table, columns *[]schema.Column, csvfile *string, strategyOpts StrategyOptions) error {
	stagingTableName := fmt.Sprintf("staging_%s_%s", destinationTable.Name, strings.ToLower(randomString(6)))

	steps := []func() error{
		func() error { return createStagingTable(destinationTable, stagingTableName) },
		func() error { return importToStagingTable(destinationTable.Source, stagingTableName, columns, csvfile) },
		func() error { return updatePrimaryTable(destinationTable, stagingTableName, strategyOpts) },
	}

	for _, step := range steps {
		err := step()
		if err != nil {
			return err
		}
	}

	return nil
}

func createDestinationTableIfNotExists(destination string, destinationTableName string, sourceTable *schema.Table, destinationTable *schema.Table) (err error) {
	fnlog := log.WithFields(log.Fields{
		"database": destination,
		"table":    destinationTableName,
	})

	exists, err := tableExists(destination, destinationTableName)
	if err != nil {
		return
	} else if exists {
		fnlog.Debug("Destination schema.Table already exists, inspecting")

		database, dbErr := connectDatabase(destination)
		if dbErr != nil {
			log.Fatal("Database Open Error:", err)
		}

		var dumpedTable *schema.Table
		dumpedTable, err = schema.DumpTableMetadata(database, destinationTableName)
		if err != nil {
			return
		}
		dumpedTable.Source = destination // TODO: smell

		*destinationTable = *dumpedTable

		return
	}

	*destinationTable = schema.Table{destination, destinationTableName, make([]schema.Column, len(sourceTable.Columns))}
	copy(destinationTable.Columns, sourceTable.Columns)

	fnlog.Infof("Destination schema.Table does not exist, creating")
	if Preview {
		log.Debug("(not executed) SQL Query:\n" + indentString(destinationTable.GenerateCreateTableStatement(destinationTableName)))
		return
	}

	return createTable(dbs[destination], destinationTableName, destinationTable)
}

func createStagingTable(destinationTable *schema.Table, stagingTableName string) (err error) {
	fnlog := log.WithFields(log.Fields{
		"database":      destinationTable.Source,
		"staging_table": stagingTableName,
	})

	database, err := connectDatabase(destinationTable.Source)
	if err != nil {
		return
	}

	query := fmt.Sprintf(GetDialect(Databases[destinationTable.Source]).CreateStagingTableQuery, destinationTable.Name, stagingTableName)

	fnlog.Debugf("Creating staging table")
	if Preview {
		log.Debugf("(not executed) SQL Query: \n\t%s", query)
		return
	}

	_, err = database.Exec(query)
	return
}

func importToStagingTable(source string, stagingTableName string, columns *[]schema.Column, csvfile *string) (err error) {
	fnlog := log.WithFields(log.Fields{
		"database":      source,
		"staging_table": stagingTableName,
	})

	if Preview {
		fnlog.Debugf("(not executed) Importing CSV into staging table")
		return
	}

	fnlog.Debugf("Importing CSV into staging table")

	return importCSV(source, stagingTableName, *csvfile, *columns)
}

func updatePrimaryTable(destinationTable *schema.Table, stagingTableName string, strategyOpts StrategyOptions) (err error) {
	fnlog := log.WithFields(log.Fields{
		"database":      destinationTable.Source,
		"staging_table": stagingTableName,
		"table":         destinationTable.Name,
	})

	database, err := connectDatabase(destinationTable.Source)
	if err != nil {
		return
	}

	var query string
	switch strategyOpts.Strategy {
	case "full", "Full":
		query = fmt.Sprintf(GetDialect(Databases[destinationTable.Source]).FullLoadQuery, destinationTable.Name, stagingTableName)
	case "modified-only", "ModifiedOnly", "Incremental":
		query = fmt.Sprintf(GetDialect(Databases[destinationTable.Source]).ModifiedOnlyLoadQuery, destinationTable.Name, stagingTableName, strategyOpts.PrimaryKey)
	}

	fnlog.Debugf("Updating primary table")
	if Preview {
		log.Debugf("(not executed) SQL Query: \n\t%s", query)
		return
	}

	_, err = database.Exec(query)
	return
}

func importCSV(source string, table string, file string, columns []schema.Column) (err error) {
	var database *sql.DB
	database, err = connectDatabase(source)
	if err != nil {
		return
	}

	switch GetDialect(Databases[source]).Key {
	case "redshift":
		err = importRedshift(database, table, file, columns, Databases[source].Options)
	case "postgres":
		err = importPostgres(database, table, file, columns)
	case "sqlite":
		err = importSqlite3(database, table, file, columns)
	default:
		err = fmt.Errorf("not implemented for this database type")
	}

	return
}

func importableColumns(destinationTable *schema.Table, sourceTable *schema.Table) []schema.Column {
	var (
		destinationOnly = make([]schema.Column, 0)
		sourceOnly      = make([]schema.Column, 0)
		both            = make([]schema.Column, 0)
	)

	destinationOnly = filterColumns(destinationTable.Columns, sourceTable.NotContainsColumnWithSameName)
	sourceOnly = filterColumns(sourceTable.Columns, destinationTable.NotContainsColumnWithSameName)
	both = filterColumns(destinationTable.Columns, sourceTable.ContainsColumnWithSameName)

	for _, column := range destinationOnly {
		log.WithFields(log.Fields{
			"column": column.Name,
		}).Warn("source table does not define column included in destination table")
	}
	for _, column := range sourceOnly {
		log.WithFields(log.Fields{
			"column": column.Name,
		}).Warn("destination table does not define column included in source table, column excluded from extract")
	}

	for _, column := range both {
		destinationColumn := filterColumns(destinationTable.Columns, func(c schema.Column) bool { return column.Name == c.Name })[0]
		sourceColumn := filterColumns(sourceTable.Columns, func(c schema.Column) bool { return column.Name == c.Name })[0]

		switch destinationColumn.DataType {
		case schema.STRING:
			if sourceColumn.Options[schema.LENGTH] != schema.MaxLength && sourceColumn.Options[schema.LENGTH] > destinationColumn.Options[schema.LENGTH] {
				log.Warnf("For string column `%s`, destination LENGTH is too short", sourceColumn.Name)
			}
		case schema.INTEGER:
			if sourceColumn.Options[schema.BYTES] > destinationColumn.Options[schema.BYTES] {
				log.Warnf("For integer column `%s`, destination SIZE is too small", sourceColumn.Name)
			}
		case schema.DECIMAL:
			if sourceColumn.Options[schema.PRECISION] > destinationColumn.Options[schema.PRECISION] {
				log.Warnf("For numeric column `%s`, destination PRECISION is too small", sourceColumn.Name)
			}
		}

	}

	return both
}

func filterColumns(columns []schema.Column, f func(column schema.Column) bool) []schema.Column {
	filtered := make([]schema.Column, 0)
	for _, c := range columns {
		if f(c) {
			filtered = append(filtered, c)
		}
	}
	return filtered
}
