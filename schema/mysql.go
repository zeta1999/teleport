package schema

import (
	"fmt"
	"strings"
)

var parseMySQLSpecialTypes = func(column sqlColumn) (DataType, map[Option]int, bool) {
	databaseTypeName := strings.ToLower(column.DatabaseTypeName())

	options := make(map[Option]int)
	switch databaseTypeName {
	case "time", "year":
		options[LENGTH] = 32
		return STRING, options, true
	}

	return "", options, false
}

var mysqlEscapeIdentifier = func(name string) string {
	return fmt.Sprintf("`%s`", name)
}

var mysqlDriverExtensions = DriverExtensions{
	ParseSpecialTypes: &parseMySQLSpecialTypes,
	EscapeIdentifier:  &mysqlEscapeIdentifier,
}
