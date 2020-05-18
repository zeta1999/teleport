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

func extractAPI(endpointName string) {
	tmpfile, err := performAPIExtraction(endpointName)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Extracted to: %s\n", tmpfile)
}

func performAPIExtraction(endpointName string) (string, error) {
	readEndpoints()
	endpoint := Endpoints[endpointName]

	if !isValidMethod(endpoint.Method) {
		log.Fatal("method not valid, allowed values: GET")
	}
	if endpoint.ResponseType != "json" {
		log.Fatal("response_type not valid, allowed values: json")
	}
	if !isValidPaginationType(endpoint.PaginationType) {
		log.Fatal("pagination_type not valid, allowed values: url-inc, none")
	}

	results, err := performAPIExtractionPaginated(endpoint)
	if err != nil {
		log.Fatal("Extract API error: ", err)
	}

	tmpfile, err := generateEndpointCSV(endpoint, results)
	if err != nil {
		log.Fatal("Export CSV error: ", err)
	}

	return tmpfile, nil
}

func performAPIExtractionPaginated(endpoint Endpoint) ([]dataObject, error) {
	thread := &starlark.Thread{}
	results := make([]dataObject, 0)
	var itr int = 0
	for {
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
			transformfile := fmt.Sprintf("%s%s", endpointsConfigDirectory, transform)
			contents, err := starlark.ExecFile(thread, transformfile, nil, nil)
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
		if endpoint.PaginationType == "none" {
			break
		}
		if endpoint.MaxPages >= 0 && itr >= endpoint.MaxPages {
			break
		}
	}

	return results, nil
}

func generateEndpointCSV(endpoint Endpoint, results []dataObject) (string, error) {
	tmpfile, err := ioutil.TempFile("/tmp/", fmt.Sprintf("extract-api-%s", endpoint.Name))
	if err != nil {
		log.Fatal(err)
	}

	headers := make([]string, 0)
	for key := range results[0] {
		headers = append(headers, key)
	}

	writer := csv.NewWriter(tmpfile)
	writeBuffer := make([]string, len(headers))
	for _, object := range results {
		fmt.Println(object)
		for i, key := range headers {
			switch object[key].(type) {
			case string:
				writeBuffer[i] = string(object[key].(string))
			case nil:
				writeBuffer[i] = ""
			default:
				fmt.Printf("%T\n", object[key])
				fmt.Println(object[key])
				writeBuffer[i] = string(object[key].([]byte))
			}
		}
		err = writer.Write(writeBuffer)
		if err != nil {
			log.Fatal(err)
		}
	}

	writer.Flush()

	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	return tmpfile.Name(), nil
}

func getResponse(method string, url string, headers map[string]string, target interface{}) error {
	client := &http.Client{}

	req, err := http.NewRequest(strings.ToUpper(method), url, nil)
	for key, value := range headers {
		req.Header.Add(key, os.ExpandEnv(value))
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("http error: ", err)
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	return decoder.Decode(target)
}

func convertJSONNumbers(data interface{}) (v interface{}, err error) {
	switch x := data.(type) {
	case json.Number:
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
