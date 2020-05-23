package main

import (
	"fmt"
	"log"
	"strings"
)

func (tc *taskContext) destinationTableName() string {
	return fmt.Sprintf("%s_%s", tc.Source, tc.TableName)
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

func createStagingTable(tc *taskContext) error {
	log.Printf("Creating staging table `staging_%s` in *%s*", tc.destinationTableName(), tc.Destination)

	_, err := dbs[tc.Destination].Exec(fmt.Sprintf(GetDialect(Connections[tc.Destination]).CreateStagingTableQuery, tc.destinationTableName()))

	return err
}

func loadDestination(tc *taskContext) error {
	log.Printf("Importing CSV into table `staging_%s` in *%s*", tc.destinationTableName(), tc.Destination)
	importCSV(tc.Destination, fmt.Sprintf("staging_%s", tc.destinationTableName()), tc.CSVFile, *tc.Columns)
	return nil
}

func promoteStagingTable(tc *taskContext) error {
	log.Printf("Promote staging table `staging_%[1]s` to primary `%[1]s` in *%[2]s*", tc.destinationTableName(), tc.Destination)

	_, err := dbs[tc.Destination].Exec(fmt.Sprintf(GetDialect(Connections[tc.Destination]).PromoteStagingTableQuery, tc.destinationTableName()))
	if err != nil {
		return err
	}

	return nil
}

func importCSV(source string, table string, file string, columns []Column) {
	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	if strings.HasPrefix(Connections[source].Config.URL, "redshift://") {
		return
	}

	switch GetDialect(Connections[source]).Key {
	case "redshift":
		importRedshift(database, table, file, columns, Connections[source].Config.Options)
	case "postgres":
		importPostgres(database, table, file, columns)
	case "sqlite":
		importSqlite3(database, table, file, columns)
	default:
		log.Fatalf("Not implemented for this database type")
	}
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
