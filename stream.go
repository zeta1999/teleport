package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

var KinesisClient kinesisiface.KinesisAPI

type changes struct {
	tables map[string]*tableChanges
}

type tableChanges struct {
	inserts_and_updates []string
	deletes             []string
}

type change struct {
	Schema       string        `json:"schema"`
	Table        string        `json:"table"`
	Kind         string        `json:"kind"`
	ColumnNames  []string      `json:"columnnames"`
	ColumnValues []interface{} `json:"columnvalues"`
	OldKeys      oldKey        `json:"oldkeys`
}

type oldKey struct {
	KeyNames  []string      `json:"keynames"`
	KeyValues []interface{} `json:"keyvalues"`
}

func (c *change) pkValue(pkColumnName string) (string, bool) {
	for idx := 0; idx < len(c.ColumnNames); idx++ {
		if c.ColumnNames[idx] == pkColumnName {
			return fmt.Sprint(c.ColumnValues[idx]), true
		}
	}

	return "", false
}

func (c *change) oldPkValue(pkColumnName string) (string, bool) {
	for idx := 0; idx < len(c.OldKeys.KeyNames); idx++ {
		if c.OldKeys.KeyNames[idx] == pkColumnName {
			return fmt.Sprint(c.OldKeys.KeyValues[idx]), true
		}
	}

	return "", false
}

func consumeLoadDatabase(sourceOrPath string, destination string) {
	var source string
	if strings.Contains(sourceOrPath, "/") {
		source = fileNameWithoutExtension(filepath.Base(sourceOrPath))
	} else {
		source = sourceOrPath
	}

	fnlog := log.WithFields(log.Fields{
		"from": source,
		"to":   destination,
	})
	fnlog.Info("Starting load-db-from-stream")

	var databaseExtract DatabaseExtract
	changes := changes{tables: make(map[string]*tableChanges)}

	ResetCurrentWorkflow()

	RunWorkflow([]func() error{
		func() error { return readDatabaseExtractConfiguration(sourceOrPath, &databaseExtract) },
		func() error { return processStream(source, &databaseExtract, &changes) },
	}, func() {
		fnlog.WithField("changes", currentWorkflow.RowCounter).Info("Processed stream")
		performUpdatesFromStream(sourceOrPath, destination, &changes)
	})
}

func processStream(source string, databaseExtract *DatabaseExtract, changes *changes) error {
	streamName, ok := Databases[source].Options["kinesis_stream_name"]
	if !ok {
		return fmt.Errorf("missing required database connection option: kinesis_stream_name")

	}
	log.WithFields(log.Fields{
		"database": source,
		"stream":   streamName,
	}).Debug("Exporting CSV of table data")

	if KinesisClient == nil {
		newSession, err := session.NewSession(aws.NewConfig())
		if err != nil {
			return err
		}

		KinesisClient = kinesis.New(newSession)
	}

	params := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String("0"),
		StreamName:        aws.String(streamName),
		ShardIteratorType: aws.String(kinesis.ShardIteratorTypeTrimHorizon),
	}

	res, err := KinesisClient.GetShardIterator(params)
	if err != nil {
		return err
	}

	shardIterator := res.ShardIterator
	records := make([]*kinesis.Record, 0)
	zeroBehindCount := 0

	for {
		res2, err := KinesisClient.GetRecords(&kinesis.GetRecordsInput{ShardIterator: shardIterator})
		if err != nil {
			return err
		}

		for _, record := range res2.Records {
			records = append(records, record)
		}

		// Break when we see MillisBehindLatest approx equal to 0 for 10 requests in a row
		if *res2.MillisBehindLatest < (5 * 60 * 1000) { // If we are within 5 minutes of the tip of the stream
			zeroBehindCount++

			if zeroBehindCount == 10 {
				break
			}
		} else {
			zeroBehindCount = 0
		}

		shardIterator = res2.NextShardIterator
	}

	for i := range records {
		var result string
		var result2 map[string][]*change
		err = json.Unmarshal(records[i].Data, &result)
		if err != nil {
			return err
		}

		err = json.Unmarshal([]byte(result), &result2)
		if err != nil {
			return err
		}

		for _, change := range result2["change"] {
			IncrementRowCounter()
			if changes.tables[change.Table] == nil {
				changes.tables[change.Table] = &tableChanges{}
			}

			if tableExtract, ok := databaseExtract.tableExtract(change.Table); ok {
				if tableExtract.LoadOptions.Strategy != Kinesis {
					continue
				}

				pkValue, ok := change.pkValue(tableExtract.LoadOptions.PrimaryKey)
				oldPkValue, oldOk := change.oldPkValue(tableExtract.LoadOptions.PrimaryKey)

				switch change.Kind {
				case "":
					continue //ignore
				case "insert":
					if ok {
						changes.tables[change.Table].inserts_and_updates = append(changes.tables[change.Table].inserts_and_updates, pkValue)
					}
				case "update":
					if ok && oldOk {
						changes.tables[change.Table].inserts_and_updates = append(changes.tables[change.Table].inserts_and_updates, pkValue)

						if pkValue != oldPkValue {
							changes.tables[change.Table].deletes = append(changes.tables[change.Table].deletes, oldPkValue)
						}
					}
				case "delete":
					if oldOk {
						changes.tables[change.Table].deletes = append(changes.tables[change.Table].deletes, oldPkValue)
					}
				default:
					log.Warnf("Unrecognized log entry: %q", change)
				}
			}
		}
	}

	return nil
}

func performUpdatesFromStream(sourceOrPath string, destination string, changes *changes) error {
	for table, tableChanges := range changes.tables {
		replicateDeletesDatabase(sourceOrPath, destination, table, tableChanges.deletes)

		StdinOverride = []byte(strings.Join(tableChanges.inserts_and_updates, "\n"))
		extractLoadDatabase(sourceOrPath, destination, table)
	}

	return nil
}
