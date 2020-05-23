package main

import (
	"reflect"
	"runtime"
	"strings"
)

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func squish(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
