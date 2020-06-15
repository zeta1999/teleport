package main

import (
	"database/sql"
	"fmt"

	log "github.com/sirupsen/logrus"
)

func load(destinationTable *Table, columns *[]Column, csvfile *string, strategyOpts StrategyOptions) error {
	stagingTableName := fmt.Sprintf("staging_%s_%s", destinationTable.Table, randomString(6))

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

func createDestinationTableIfNotExists(destination string, destinationTableName string, sourceTable *Table, destinationTable *Table) (err error) {
	fnlog := log.WithFields(log.Fields{
		"database": destination,
		"table":    destinationTableName,
	})

	exists, err := tableExists(destination, destinationTableName)
	if err != nil {
		return
	} else if exists {
		fnlog.Debug("Destination Table already exists, inspecting")

		var dumpedTable *Table
		dumpedTable, err = dumpTableMetadata(destination, destinationTableName)
		if err != nil {
			return
		}

		*destinationTable = *dumpedTable

		return
	}

	*destinationTable = Table{destination, destinationTableName, make([]Column, len(sourceTable.Columns))}
	copy(destinationTable.Columns, sourceTable.Columns)

	fnlog.Infof("Destination Table does not exist, creating")
	if Preview {
		log.Debug("(not executed) SQL Query:\n" + indentString(destinationTable.generateCreateTableStatement(destinationTableName)))
		return
	}

	return createTable(dbs[destination], destinationTableName, destinationTable)
}

func createStagingTable(destinationTable *Table, stagingTableName string) (err error) {
	fnlog := log.WithFields(log.Fields{
		"database":      destinationTable.Source,
		"staging_table": stagingTableName,
	})

	database, err := connectDatabase(destinationTable.Source)
	if err != nil {
		return
	}

	query := fmt.Sprintf(GetDialect(Databases[destinationTable.Source]).CreateStagingTableQuery, destinationTable.Table, stagingTableName)

	fnlog.Debugf("Creating staging table")
	if Preview {
		log.Debugf("(not executed) SQL Query: \n\t%s", query)
		return
	}

	_, err = database.Exec(query)

	return
}

func importToStagingTable(source string, stagingTableName string, columns *[]Column, csvfile *string) (err error) {
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

func updatePrimaryTable(destinationTable *Table, stagingTableName string, strategyOpts StrategyOptions) (err error) {
	fnlog := log.WithFields(log.Fields{
		"database":      destinationTable.Source,
		"staging_table": stagingTableName,
		"table":         destinationTable.Table,
	})

	database, err := connectDatabase(destinationTable.Source)
	if err != nil {
		return
	}

	var query string
	switch strategyOpts.Strategy {
	case "full":
		query = fmt.Sprintf(GetDialect(Databases[destinationTable.Source]).FullLoadQuery, destinationTable.Table, stagingTableName)
	case "modified-only":
		query = fmt.Sprintf(GetDialect(Databases[destinationTable.Source]).FullLoadQuery, destinationTable.Table, stagingTableName, strategyOpts.PrimaryKey)
	}

	fnlog.Debugf("Updating primary table")
	if Preview {
		log.Debugf("(not executed) SQL Query: \n\t%s", query)
		return
	}

	_, err = database.Exec(query)
	return
}

func importCSV(source string, table string, file string, columns []Column) (err error) {
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

func importableColumns(destinationTable *Table, sourceTable *Table) []Column {
	var (
		destinationOnly = make([]Column, 0)
		sourceOnly      = make([]Column, 0)
		both            = make([]Column, 0)
	)

	destinationOnly = filterColumns(destinationTable.Columns, sourceTable.notContainsColumnWithSameName)
	sourceOnly = filterColumns(sourceTable.Columns, destinationTable.notContainsColumnWithSameName)
	both = filterColumns(destinationTable.Columns, sourceTable.containsColumnWithSameName)

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
		destinationColumn := filterColumns(destinationTable.Columns, func(c Column) bool { return column.Name == c.Name })[0]
		sourceColumn := filterColumns(sourceTable.Columns, func(c Column) bool { return column.Name == c.Name })[0]

		switch destinationColumn.DataType {
		case STRING:
			if sourceColumn.Options[LENGTH] > destinationColumn.Options[LENGTH] {
				log.Warnf("For string column `%s`, destination LENGTH is too short", sourceColumn.Name)
			}
		case INTEGER:
			if sourceColumn.Options[BYTES] > destinationColumn.Options[BYTES] {
				log.Warnf("For integer column `%s`, destination SIZE is too small", sourceColumn.Name)
			}
		case DECIMAL:
			if sourceColumn.Options[PRECISION] > destinationColumn.Options[PRECISION] {
				log.Warnf("For numeric column `%s`, destination PRECISION is too small", sourceColumn.Name)
			}
		}

	}

	return both
}

func filterColumns(columns []Column, f func(column Column) bool) []Column {
	filtered := make([]Column, 0)
	for _, c := range columns {
		if f(c) {
			filtered = append(filtered, c)
		}
	}
	return filtered
}

func (table *Table) containsColumnWithSameName(c Column) bool {
	for _, column := range table.Columns {
		if c.Name == column.Name {
			return true
		}
	}
	return false
}

func (table *Table) notContainsColumnWithSameName(c Column) bool {
	for _, column := range table.Columns {
		if c.Name == column.Name {
			return false
		}
	}
	return true
}
