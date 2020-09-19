package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
)

func updateTransform(source string, transformName string) {
	var contents string
	var ok bool
	if contents, ok = SQLTransforms[transformName+".sql"]; !ok {
		raw, err := ioutil.ReadFile(filepath.Join(workingDir(), transformsConfigDirectory, transformName+".sql"))
		if err != nil {
			log.Fatal(err)
		}

		contents = string(raw)
	}

	db, err := connectDatabase(source)
	if err != nil {
		log.Fatal("Database Connect Error:", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
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
