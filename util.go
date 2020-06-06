package main

import (
	"strings"
)

func squish(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func indentString(s string) string {
	return "\t" + strings.Join(strings.Split(s, "\n"), "\n\t")
}
