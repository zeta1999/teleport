package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	slutil "github.com/qri-io/starlib/util"
	"go.starlark.net/starlark"
)

func extractAPI(endpointName string) {
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

	thread := &starlark.Thread{}
	results := make([]interface{}, 0)
	var itr int = 0
	for {
		currentURL := strings.NewReplacer("%(page)", strconv.Itoa(itr)).Replace(endpoint.URL)
		var target interface{}
		getResponse(endpoint.Method, currentURL, endpoint.Headers, &target)
		value, err := slutil.Marshal(target)
		if err != nil {
			log.Fatal("unable to parse response: ", err)
		}

		for _, transform := range endpoint.Transforms {
			transformfile := fmt.Sprintf("%s%s", endpointsConfigDirectory, transform)
			contents, err := starlark.ExecFile(thread, transformfile, nil, nil)
			if err != nil {
				log.Fatalf("read starlark file `%s` error: %s", transform, err)
			}

			value, err = starlark.Call(thread, contents["transform"], starlark.Tuple{value}, nil)
			if err != nil {
				log.Fatalf("transform `%s` error: %s", transform, err)
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
					log.Fatal("read object error: ", err)
				}

				results = append(results, object)
			}
		case *starlark.Dict:
			object, err := slutil.Unmarshal(value)
			if err != nil {
				log.Fatal("read object error: ", err)
			}
			results = append(results, object)
		}

		itr++
		if endpoint.PaginationType == "none" {
			break
		}
		if endpoint.MaxPages >= 0 && itr >= endpoint.MaxPages {
			break
		}
	}

	fmt.Println(results)
	fmt.Println(len(results))
	// TODO: export results to CSV (how to do columns??)
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

	return json.NewDecoder(resp.Body).Decode(target)
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
