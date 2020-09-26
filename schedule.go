package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strconv"

	"go.starlark.net/starlark"
)

var (
	schedule = make([]commandSchedule, 0)
)

type commandSchedule interface {
	toMap() map[string]interface{}
}

type rate struct {
	value int
	unit  string
}

type extractLoadAPISchedule struct {
	rate       rate
	fromSource string
	toSource   string
}

type extractLoadDBSchedule struct {
	rate       rate
	fromSource string
	table      string
	toSource   string
}

type transformSchedule struct {
	rate   rate
	source string
	table  string
}

func readSchedule() error {
	portFile, err := findPortFile("schedule", []string{configDirectory})
	if err != nil {
		return err
	}

	_, err = starlark.ExecFile(&starlark.Thread{}, portFile, nil, scheduleDSL())
	if err != nil {
		return appendBackTraceToStarlarkError(err)
	}

	return nil
}

func exportSchedule() ([]byte, error) {
	formatted := make([]map[string]interface{}, 0)
	for _, item := range schedule {
		formatted = append(formatted, item.toMap())
	}

	return json.Marshal(formatted)
}

func scheduleDSL() starlark.StringDict {
	return starlark.StringDict{
		// DSL Methods
		"ExtractLoadAPI": starlark.NewBuiltin("ExtractLoadAPI", scheduleExtractLoadAPI),
		"ExtractLoadDB":  starlark.NewBuiltin("ExtractLoadDB", scheduleExtractLoadDB),
		"Transform":      starlark.NewBuiltin("Transform", scheduleTransform),
	}
}

func scheduleExtractLoadAPI(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (value starlark.Value, err error) {
	item := extractLoadAPISchedule{}
	var every starlark.String

	if err := starlark.UnpackArgs("ExtractLoadAPI", args, kwargs, "from", &item.fromSource, "every", &every, "to", &item.toSource); err != nil {
		return nil, prependStarlarkPositionToError(thread, err)
	}

	if err := item.rate.parse(every.GoString()); err != nil {
		return nil, prependStarlarkPositionToError(thread, err)
	}

	if err := item.validate(); err != nil {
		return nil, prependStarlarkPositionToError(thread, err)
	}

	schedule = append(schedule, &item)

	return starlark.None, nil
}

func scheduleExtractLoadDB(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (value starlark.Value, err error) {
	item := extractLoadDBSchedule{}
	var every starlark.String

	if err := starlark.UnpackArgs("ExtractLoadDB", args, kwargs, "from", &item.fromSource, "table", &item.table, "every", &every, "to", &item.toSource); err != nil {
		return nil, prependStarlarkPositionToError(thread, err)
	}

	if err := item.rate.parse(every.GoString()); err != nil {
		return nil, prependStarlarkPositionToError(thread, err)
	}

	if err := item.validate(); err != nil {
		return nil, prependStarlarkPositionToError(thread, err)
	}

	schedule = append(schedule, &item)

	return starlark.None, nil
}

func scheduleTransform(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (value starlark.Value, err error) {
	item := transformSchedule{}
	var every starlark.String

	if err := starlark.UnpackArgs("Transform", args, kwargs, "table", &item.table, "every", &every, "source", &item.source); err != nil {
		return nil, prependStarlarkPositionToError(thread, err)
	}

	if err := item.rate.parse(every.GoString()); err != nil {
		return nil, prependStarlarkPositionToError(thread, err)
	}

	if err := item.validate(); err != nil {
		return nil, prependStarlarkPositionToError(thread, err)
	}

	schedule = append(schedule, &item)

	return starlark.None, nil
}

func (api *extractLoadAPISchedule) validate() error {
	if err := validAPISource(api.fromSource); err != nil {
		return err
	}

	if err := validLoadDestination(api.toSource); err != nil {
		return err
	}

	return nil
}

func (api *extractLoadAPISchedule) toMap() map[string]interface{} {
	return map[string]interface{}{
		"command": []string{"extract-load-api", "-from", api.fromSource, "-to", api.toSource},
		"rate":    api.rate.toMap(),
	}
}

func (db *extractLoadDBSchedule) validate() error {
	if err := validDBSource(db.fromSource); err != nil {
		return err
	}

	if err := validLoadDestination(db.toSource); err != nil {
		return err
	}

	return nil
}

func (db *extractLoadDBSchedule) toMap() map[string]interface{} {
	return map[string]interface{}{
		"command": []string{"extract-load-db", "-from", db.fromSource, "-table", db.table, "-to", db.toSource},
		"rate":    db.rate.toMap(),
	}
}

func (transform *transformSchedule) validate() error {
	if _, ok := SQLTransforms[transform.table+".sql"]; !ok {
		if _, err := ioutil.ReadFile(filepath.Join(workingDir(), transformsConfigDirectory, transform.table+".sql")); err != nil {
			return err
		}
	}

	if err := validLoadDestination(transform.source); err != nil {
		return err
	}

	return nil
}

func (transform *transformSchedule) toMap() map[string]interface{} {
	return map[string]interface{}{
		"command": []string{"transform", "-source", transform.source, "-table", transform.table},
		"rate":    transform.rate.toMap(),
	}
}

func (rate *rate) parse(input string) error {
	rateRegex := regexp.MustCompile(`^([1-9]\d*) (minute|hour|day)s?$`)

	if matches := rateRegex.FindStringSubmatch(input); len(matches) == 3 {
		rate.value, _ = strconv.Atoi(matches[1]) // Since the regex is matching (\d+), we never expect an error here
		rate.unit = matches[2]
		return nil
	}

	return fmt.Errorf("unable to parse `every` argument: \"%s\"", input)
}

func (rate *rate) toMap() map[string]interface{} {
	return map[string]interface{}{
		"value": rate.value,
		"unit":  rate.unit,
	}
}

func validAPISource(source string) error {
	return readEndpointConfiguration(source, &Endpoint{})
}

func validDBSource(source string) error {
	if _, ok := Databases[source]; !ok {
		return fmt.Errorf("`from` does not refer to a valid database connection: \"%s\"", source)
	}

	return nil
}

func validLoadDestination(source string) error {
	if database, ok := Databases[source]; !ok {
		return fmt.Errorf("`to` does not refer to a valid database connection: \"%s\"", source)
	} else if database.Readonly {
		return fmt.Errorf("`to` cannot refer to a Readonly database connection: \"%s\"", source)
	}

	return nil
}
