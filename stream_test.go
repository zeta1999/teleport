package main

import (
	"io/ioutil"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestConsumeKinesisStream(t *testing.T) {
	KinesisClient = &mockKinesisClient{}
	defer func() { KinesisClient = nil }()

	Databases["testsrc"] = Database{"postgres://postgres@localhost:45432/?sslmode=disable", map[string]string{"kinesis_stream_name": "Foo"}, true}
	srcdb, err := connectDatabase("testsrc")
	if err != nil {
		assert.FailNow(t, "%w", err)
	}
	defer delete(dbs, "testsrc")
	defer srcdb.Exec(`DROP TABLE IF EXISTS widgets`)

	Databases["testdest"] = Database{"sqlite://:memory:", map[string]string{}, true}
	destdb, err := connectDatabase("testdest")
	if err != nil {
		assert.FailNow(t, "%w", err)
	}
	defer delete(dbs, "testdest")

	redirectLogs(t, func() {
		// Initial Full History Re-Sync
		srcdb.Exec(srcdb.GenerateCreateTableStatement("widgets", widgetsTableDefinition))
		err = importCSV("testsrc", "widgets", "testdata/example_widgets.csv", widgetsTableDefinition.Columns)
		assert.NoError(t, err)
		extractLoadDatabase("testsrc", "testdest", "widgets")

		// Changes in source (reflected in tesdata/kinesis_records.txt)
		_, err = srcdb.Exec(`INSERT INTO widgets (id) VALUES (11)`)
		assert.NoError(t, err)
		_, err = srcdb.Exec(`UPDATE widgets SET id = 12 WHERE id = 4`)
		assert.NoError(t, err)
		_, err = srcdb.Exec(`DELETE FROM widgets WHERE id IN (2,3,7)`)
		assert.NoError(t, err)
		_, err = srcdb.Exec(`INSERT INTO widgets (id) VALUES (7)`) // Re-insert 7 after deleting it
		assert.NoError(t, err)

		withTestDatabasePortFile(t, "testsrc", "stream_kinesis.port", func(t *testing.T, filename string) {
			consumeLoadDatabase(filename, "testdest")
			assert.Equal(t, log.InfoLevel, hook.LastEntry().Level)

			// 10 original rows
			// 1 insert (+1)
			// 1 update with key change (+1/-1 = no change)
			// 1 update (no change)
			// 3 deletes (-3)
			// 1 re-insert (+1)
			assertRowCount(t, 9, destdb, "testsrc_widgets")

			var idsRaw string
			destdb.QueryRow("SELECT GROUP_CONCAT(id, ',') FROM testsrc_widgets").Scan(&idsRaw)
			ids := strings.Split(idsRaw, ",")
			sort.Strings(ids)
			assert.Equal(t, []string{"1", "10", "11", "12", "5", "6", "7", "8", "9"}, ids)
		})
	})
}

type mockKinesisClient struct {
	kinesisiface.KinesisAPI
}

func (m *mockKinesisClient) GetShardIterator(input *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	return &kinesis.GetShardIteratorOutput{
		ShardIterator: aws.String("d8B1/uLCvOOY8t6LuHZAue1XguVsZa9sqk/CFPj0Bh1ndMVLFa/I8h2eTGB5EzDp"),
	}, nil
}

func (m *mockKinesisClient) GetRecords(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	testData, err := ioutil.ReadFile("testdata/kinesis_records.txt")
	if err != nil {
		panic(err)
	}
	dataValues := strings.Split(string(testData), "\n")
	records := make([]*kinesis.Record, 0)
	for _, data := range dataValues {
		if data == "" {
			continue
		}
		records = append(records, &kinesis.Record{Data: []byte(data)})
	}
	return &kinesis.GetRecordsOutput{
		NextShardIterator:  aws.String("ymhQf2cW9FcGFuGZ2qY1jfojnMzdyl6k3h708HwU9DMVJ655ESxiwkei2FnScE3p"),
		MillisBehindLatest: aws.Int64(60000), // 1 minute
		Records:            records,
	}, nil
}
