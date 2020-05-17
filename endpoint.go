package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	slutil "github.com/qri-io/starlib/util"
	"go.starlark.net/starlark"
)

func extractAPI(endpointName string) {
	readEndpoints()
	endpoint := Endpoints[endpointName]

	if endpoint.Method != "GET" {
		log.Fatal("method not valid, allowed values: GET")
	}
	if endpoint.ResponseType != "json" {
		log.Fatal("response_type not valid, allowed values: json")
	}
	if endpoint.PaginationType != "url-inc" {
		log.Fatal("pagination_type not valid, allowed values: url-inc")
	}

	thread := &starlark.Thread{}
	results := make([]interface{}, 0)
	var itr int = 0
	for {
		currentURL := strings.NewReplacer("%(page)", strconv.Itoa(itr)).Replace(endpoint.URL)
		var target interface{}
		getResponse(currentURL, &target)
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

		itr++
		if endpoint.MaxPages >= 0 && itr >= endpoint.MaxPages {
			break
		}
	}

	fmt.Println(results)
	fmt.Println(len(results))
	// TODO: export results to CSV (how to do columns??)
}

func getResponse(url string, target interface{}) error {
	resp, err := http.Get(url)
	if err != nil {
		log.Fatal("http error: ", err)
	}
	defer resp.Body.Close()

	return json.NewDecoder(resp.Body).Decode(target)
}
