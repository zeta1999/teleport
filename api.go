package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	slutil "github.com/qri-io/starlib/util"
	"go.starlark.net/starlark"
)

type dataObject = map[string]interface{}

var emptyResults = make([]dataObject, 0)

func extractLoadAPI(endpoint string, destination string, tableName string, strategy string, strategyOpts map[string]string) {
	log.Printf("Starting extract-load-api from *%s* to *%s* table `%s`", endpoint, destination, tableName)

	var destinationTable Table
	var columns []Column
	var results []dataObject
	var csvfile string

	destinationTableName := fmt.Sprintf("%s_%s", endpoint, tableName)

	runWorkflow([]func() error{
		func() error { return connectDatabaseWithLogging(destination) },
		func() error { return inspectTable(destination, destinationTableName, &destinationTable) },
		func() error { return performAPIExtraction(endpoint, &results) },
		func() error { return determineImportColumns(&destinationTable, results, &columns) },
		func() error { return saveResultsToCSV(endpoint, results, &columns, &csvfile) },
		func() error { return createStagingTable(&destinationTable) },
		func() error { return loadDestination(&destinationTable, &columns, &csvfile) },
		func() error { return promoteStagingTable(&destinationTable) },
	})
}

func extractAPI(endpoint string) {
	log.Printf("Starting extract-api from *%s*", endpoint)

	var results []dataObject
	var csvfile string

	runWorkflow([]func() error{
		func() error { return performAPIExtraction(endpoint, &results) },
		func() error { return saveResultsToCSV(endpoint, results, nil, &csvfile) },
	})

	log.Printf("Extracted to: %s\n", csvfile)
}

func determineImportColumns(destinationTable *Table, results []dataObject, columns *[]Column) error {
	headers := make([]string, 0)
	for key := range results[0] {
		headers = append(headers, key)
	}

	importColumns := make([]Column, 0)
	for _, column := range destinationTable.Columns {
		for _, header := range headers {
			if column.Name == header {
				importColumns = append(importColumns, column)
				break
			}
		}
	}

	*columns = importColumns

	return nil
}

func performAPIExtraction(endpointName string, results *[]dataObject) error {
	endpoint := Endpoints[endpointName]

	if !isValidMethod(endpoint.Method) {
		return fmt.Errorf("method not valid, allowed values: GET")
	}
	if endpoint.ResponseType != "json" {
		return fmt.Errorf("response_type not valid, allowed values: json")
	}
	if !isValidPaginationType(endpoint.PaginationType) {
		return fmt.Errorf("pagination_type not valid, allowed values: url-inc, none")
	}

	extractedResults, err := performAPIExtractionPaginated(endpoint)
	if err != nil {
		return err
	}

	*results = extractedResults

	return nil
}

func performAPIExtractionPaginated(endpoint Endpoint) ([]dataObject, error) {
	thread := &starlark.Thread{}
	results := make([]dataObject, 0)
	var itr int = 0
	for {
		log.Printf("Requesting page %d", itr)
		currentURL := strings.NewReplacer("%(page)", strconv.Itoa(itr)).Replace(endpoint.URL)
		var target interface{}
		getResponse(endpoint.Method, currentURL, endpoint.Headers, &target)
		converted, err := convertJSONNumbers(target)
		if err != nil {
			return emptyResults, fmt.Errorf("unable to parse response: %w", err)
		}
		value, err := slutil.Marshal(converted)
		if err != nil {
			return emptyResults, fmt.Errorf("unable to parse response: %w", err)
		}

		for _, transform := range endpoint.Transforms {
			log.Printf("Applying transform: *%s*", transform)
			var contents starlark.StringDict
			if source, ok := Transforms[transform]; ok {
				contents, err = starlark.ExecFile(thread, transform, source, nil)
			} else {
				transformfile := fmt.Sprintf("%s%s", endpointsConfigDirectory, transform)
				contents, err = starlark.ExecFile(thread, transformfile, nil, nil)
			}
			if err != nil {
				return emptyResults, fmt.Errorf("read starlark file `%s` error: %w", transform, err)
			}

			value, err = starlark.Call(thread, contents["transform"], starlark.Tuple{value}, nil)
			if err != nil {
				return emptyResults, fmt.Errorf("transform `%s` error: %s", transform, err)
			}
		}

		switch value.(type) {
		case *starlark.List:
			objectItr := value.(*starlark.List).Iterate()
			var slobject starlark.Value
			defer objectItr.Done()
			for objectItr.Next(&slobject) {
				object, err := slutil.Unmarshal(slobject)
				if err != nil {
					return emptyResults, fmt.Errorf("read object error: %w", err)
				}

				results = append(results, object.(dataObject))
			}
		case *starlark.Dict:
			object, err := slutil.Unmarshal(value)
			if err != nil {
				return emptyResults, fmt.Errorf("read object error: %w", err)
			}
			results = append(results, object.(dataObject))
		}

		itr++
		if Preview {
			if len(results) > PreviewLimit {
				results = results[:PreviewLimit]
			}
			log.Printf("[PREVIEW] Skipping additional pages if any")
			break
		}
		if endpoint.PaginationType == "none" {
			break
		}
		if endpoint.MaxPages >= 0 && itr >= endpoint.MaxPages {
			break
		}
	}

	return results, nil
}

func saveResultsToCSV(endpointName string, results []dataObject, columns *[]Column, csvfile *string) error {
	tmpfile, err := ioutil.TempFile("/tmp/", fmt.Sprintf("extract-api-%s", endpointName))
	if err != nil {
		return err
	}

	headers := make([]string, 0)
	if columns == nil {
		for key := range results[0] {
			headers = append(headers, key)
		}
	} else {
		for _, column := range *columns {
			headers = append(headers, column.Name)
		}
	}

	writer := csv.NewWriter(tmpfile)
	writeBuffer := make([]string, len(headers))

	for _, object := range results {
		for i, key := range headers {
			switch object[key].(type) {
			case string:
				writeBuffer[i] = string(object[key].(string))
			case nil:
				writeBuffer[i] = ""
			default:
				writeBuffer[i] = string(object[key].([]byte))
			}
		}
		err = writer.Write(writeBuffer)
		if err != nil {
			return err
		}
	}

	writer.Flush()

	if err := tmpfile.Close(); err != nil {
		return err
	}

	if Preview {
		content, err := ioutil.ReadFile(tmpfile.Name())
		if err != nil {
			return err
		}

		log.Printf(`[PREVIEW] Results CSV (limit: %d)
Headers:
%s

Body:
%s
		`, PreviewLimit, strings.Join(headers, ","), string(content))
	}

	*csvfile = tmpfile.Name()

	return nil
}

func getResponse(method string, url string, headers map[string]string, target interface{}) error {
	client := &http.Client{}

	req, err := http.NewRequest(strings.ToUpper(method), url, nil)
	for key, value := range headers {
		req.Header.Add(key, os.ExpandEnv(value))
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http error: %w", err)
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	return decoder.Decode(target)
}

func convertJSONNumbers(data interface{}) (v interface{}, err error) {
	switch x := data.(type) {
	case json.Number:
		// Use String representation of number for now since the destination database is defining types
		v = x.String()
	case []interface{}:
		var elems = make([]interface{}, len(x))
		for i, val := range x {
			elems[i], err = convertJSONNumbers(val)
			if err != nil {
				return
			}
		}
		v = elems
	case map[interface{}]interface{}:
		dict := make(map[interface{}]interface{})
		var elem interface{}
		for key, val := range x {
			elem, err = convertJSONNumbers(val)
			if err != nil {
				return
			}
			dict[key] = elem
		}
		v = dict
	case map[string]interface{}:
		dict := make(map[interface{}]interface{})
		var elem interface{}
		for key, val := range x {
			elem, err = convertJSONNumbers(val)
			if err != nil {
				return
			}
			dict[key] = elem
		}
		v = dict
	default:
		v = x
	}
	return
}

func isValidPaginationType(paginationType string) bool {
	switch paginationType {
	case
		"",
		"url-inc",
		"none":
		return true
	}
	return false
}

func isValidMethod(method string) bool {
	switch strings.ToUpper(method) {
	case
		"GET",
		"POST":
		return true
	}
	return false
}
