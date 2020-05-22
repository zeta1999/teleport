package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"strings"
)

func updateTransform(source string, transformName string) {
	contents, err := ioutil.ReadFile(strings.Join([]string{"transforms/", transformName, ".sql"}, ""))
	if err != nil {
		log.Fatal(err)
	}

	database, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Connect Error:", err)
	}

	_, err = database.Exec(fmt.Sprintf(`
		DROP TABLE IF EXISTS staging_%s;

		CREATE TABLE staging_%s AS %s;

		BEGIN;
			DROP TABLE IF EXISTS %s;
			ALTER TABLE staging_%s RENAME TO %s;
		END;
	`, transformName, transformName, contents, transformName, transformName, transformName))
	if err != nil {
		log.Fatal("Transform Error:", err)
	}
}
