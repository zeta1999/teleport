package main

import (
	"database/sql"
	"fmt"
	"log"
)

func createDestinationTableIfNotExists(destination string, destinationTableName string, sourceTable *Table, destinationTable *Table) (err error) {
	exists, err := tableExists(destination, destinationTableName)
	if err != nil {
		return
	} else if exists {
		log.Printf("Table `%s` already exists in *%s*, describing", destinationTableName, destination)

		var dumpedTable *Table
		dumpedTable, err = dumpTableMetadata(destination, destinationTableName)
		if err != nil {
			return
		}

		*destinationTable = *dumpedTable

		return
	}

	log.Printf("Table `%s` does not exist in *%s*, creating", destinationTableName, destination)

	*destinationTable = Table{destination, destinationTableName, make([]Column, len(sourceTable.Columns))}
	copy(destinationTable.Columns, sourceTable.Columns)

	return createTable(dbs[destination], destinationTableName, destinationTable)
}

func createStagingTable(destinationTable *Table) error {
	log.Printf("Creating staging table `staging_%s` in *%s*", destinationTable.Table, destinationTable.Source)

	_, err := dbs[destinationTable.Source].Exec(fmt.Sprintf(GetDialect(Connections[destinationTable.Source]).CreateStagingTableQuery, destinationTable.Table))

	return err
}

func loadDestination(destinationTable *Table, columns *[]Column, csvfile *string) error {
	log.Printf("Importing CSV into table `staging_%s` in *%s*", destinationTable.Table, destinationTable.Source)

	return importCSV(destinationTable.Source, fmt.Sprintf("staging_%s", destinationTable.Table), *csvfile, *columns)
}

func promoteStagingTable(destinationTable *Table) error {
	log.Printf("Promote staging table `staging_%[1]s` to primary `%[1]s` in *%[2]s*", destinationTable.Table, destinationTable.Source)

	_, err := dbs[destinationTable.Source].Exec(fmt.Sprintf(GetDialect(Connections[destinationTable.Source]).PromoteStagingTableQuery, destinationTable.Table))
	return err
}

func importCSV(source string, table string, file string, columns []Column) (err error) {
	var database *sql.DB
	database, err = connectDatabase(source)
	if err != nil {
		return
	}

	switch GetDialect(Connections[source]).Key {
	case "redshift":
		err = importRedshift(database, table, file, columns, Connections[source].Config.Options)
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
		log.Printf("destination table column `%s` excluded from extract (not present in source)", column.Name)
	}
	for _, column := range sourceOnly {
		log.Printf("source table column `%s` excluded from extract (not present in destination)", column.Name)
	}

	for _, column := range both {
		destinationColumn := filterColumns(destinationTable.Columns, func(c Column) bool { return column.Name == c.Name })[0]
		sourceColumn := filterColumns(sourceTable.Columns, func(c Column) bool { return column.Name == c.Name })[0]

		switch destinationColumn.DataType {
		case STRING:
			if sourceColumn.Options[LENGTH] > destinationColumn.Options[LENGTH] {
				log.Printf("For string column `%s`, destination LENGTH is too short", sourceColumn.Name)
			}
		case INTEGER:
			if sourceColumn.Options[BYTES] > destinationColumn.Options[BYTES] {
				log.Printf("For integer column `%s`, destination SIZE is too small", sourceColumn.Name)
			}
		case DECIMAL:
			if sourceColumn.Options[PRECISION] > destinationColumn.Options[PRECISION] {
				log.Printf("For numeric column `%s`, destination PRECISION is too small", sourceColumn.Name)
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
