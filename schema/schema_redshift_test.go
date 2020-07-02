// +build redshift

package schema

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

func TestRedshiftInspection(t *testing.T) {
	withDb(t, os.ExpandEnv("redshift://$TEST_REDSHIFT_USER:$TEST_REDSHIFT_PASSWORD@$TEST_REDSHIFT_HOST:5439/dev"), func(db *sql.DB) {
		testColumnCases(t, db, genericCases)

		testColumnCases(t, db, genericStringCases)

		// Special Types
		testColumnCases(t, db, []struct {
			originalDataTypes  []string
			column             Column
			createTabeDataType string
		}{
			{
				[]string{"text"},
				Column{"", STRING, map[Option]int{LENGTH: 256}},
				"VARCHAR(256)",
			},
		})
	})
}
