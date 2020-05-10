package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jimsmart/schema"
	"github.com/lib/pq"
)

func importCSV(source string, table string, file string) {
	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Open Error:", err)
	}

	if strings.HasPrefix(Connections[source].Config.Url, "redshift://") {
		importRedshift(database, table, file, Connections[source].Config.Options)
		return
	}

	switch driverType := fmt.Sprintf("%T", database.Driver()); driverType {
	case "*pq.Driver":
		importPostgres(database, table, file)
	case "*sqlite3.SQLiteDriver":
		importSqlite3(database, table, file)
	}
}

func importRedshift(database *sql.DB, table string, file string, options map[string]string) {
	log.Print("Uploading CSV to S3")
	s3URL, err := uploadFileToS3(options["s3_bucket"], file)
	if err != nil {
		log.Fatal("S3 Upload Error: ", err)
	}

	log.Print("Executing Redshift COPY command")
	_, err = database.Exec(fmt.Sprintf(`
		COPY %s
		FROM '%s'
		IAM_ROLE '%s'
		REGION '%s'
		CSV
		EMPTYASNULL
		ACCEPTINVCHARS
		;
	`, table, s3URL, options["service_role"], options["s3_region"]))

	if err != nil {
		log.Fatal("Redshift Copy Error: ", err)
	}
}

func uploadFileToS3(bucket string, filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}

	keyElements := []string{"teleport", time.Now().Format(time.RFC3339), filepath.Base(filename)}
	key := strings.Join(keyElements, "/")

	svc := s3.New(session.New())
	input := &s3.PutObjectInput{
		Body:   file,
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	_, err = svc.PutObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			return "", aerr
		}
		return "", err
	}

	return fmt.Sprintf("s3://%s/%s", bucket, key), nil
}

func importPostgres(database *sql.DB, table string, file string) {
	transaction, err := database.Begin()
	if err != nil {
		log.Fatal(err)
	}

	columns, err := schema.Table(database, table)
	if err != nil {
		log.Fatal(err)
	}

	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name()
	}

	statement, err := transaction.Prepare(pq.CopyIn(table, columnNames...))
	if err != nil {
		log.Fatal(err)
	}

	csvfile, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	reader := csv.NewReader(bufio.NewReader(csvfile))

	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}

		writeBuffer := make([]interface{}, len(line))
		for i, value := range line {
			if value == "" { // Assume a blank cell is NULL
				writeBuffer[i] = nil
			} else {
				writeBuffer[i] = value
			}
		}

		_, err = statement.Exec(writeBuffer...)
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err = statement.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = statement.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = transaction.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

func importSqlite3(database *sql.DB, table string, file string) {
	columns, err := schema.Table(database, table)
	if err != nil {
		log.Fatalf("Table error: %s", err)
	}

	transaction, err := database.Begin()
	if err != nil {
		log.Fatal(err)
	}

	preparedStatement := fmt.Sprintf("INSERT INTO %s (", table)
	for _, column := range columns {
		preparedStatement += fmt.Sprintf("%s, ", column.Name())
	}
	preparedStatement = strings.TrimSuffix(preparedStatement, ", ")

	preparedStatement += ") VALUES ("
	for range columns {
		preparedStatement += "?, "
	}
	preparedStatement = strings.TrimSuffix(preparedStatement, ", ")
	preparedStatement += ");"

	statement, err := transaction.Prepare(preparedStatement)
	if err != nil {
		log.Fatal(err)
	}

	csvfile, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	reader := csv.NewReader(bufio.NewReader(csvfile))
	if err != nil {
		log.Fatal(err)
	}

	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatalf("Line error: %s", error)
		}

		writeBuffer := make([]interface{}, len(line))
		for i, value := range line {
			if value == "" { // Assume a blank cell is NULL
				writeBuffer[i] = nil
			} else {
				writeBuffer[i] = value
			}
		}

		_, err = statement.Exec(writeBuffer...)
		if err != nil {
			log.Fatal("Import statement exec error: ", err)
		}
	}

	err = statement.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = transaction.Commit()
	if err != nil {
		log.Fatal(err)
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
