package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSchedule(t *testing.T) {
	os.Setenv("PADPATH", "testdata/pad")
	defer os.Unsetenv("PADPATH")

	readDatabaseConnectionConfiguration()

	err := readSchedule()
	assert.NoError(t, err)

	marshalled, err := exportSchedule()
	assert.NoError(t, err)

	var unmarshalled []map[string]interface{}
	err = json.Unmarshal(marshalled, &unmarshalled)
	assert.NoError(t, err)

	assert.Len(t, unmarshalled, 4)

	apiSchedule := unmarshalled[0]["command"].([]interface{})
	assert.Equal(t, "extract-load-api", apiSchedule[0])
	assert.Equal(t, "worldtimeapi_ip_times", apiSchedule[2])
	assert.Equal(t, "postgresdocker", apiSchedule[4])

	dbSchedule := unmarshalled[2]["command"].([]interface{})
	assert.Equal(t, "extract-load-db", dbSchedule[0])
	assert.Equal(t, "example", dbSchedule[2])
	assert.Equal(t, "objects", dbSchedule[4])
	assert.Equal(t, "postgresdocker", dbSchedule[6])

	transformSchedule := unmarshalled[3]["command"].([]interface{})
	assert.Equal(t, "transform", transformSchedule[0])
	assert.Equal(t, "postgresdocker", transformSchedule[2])
	assert.Equal(t, "times_by_day_of_week", transformSchedule[4])
}
