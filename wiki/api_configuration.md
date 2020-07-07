Teleport uses its own "Port" declarative configuration language for configuring API endpoints for extracting.
Each ".port" file contains the configuration for one API endpoint. "Port" is a python dialect, so you
can configure your test editor's syntax highlighting to use "Python" formatting for ".port" files.

Here's an example "Port" file for the [Holiday API](https://holidayapi.com/docs)

```python
Get("https://holidayapi.com/v1/holidays?key=$HOLIDAY_API_KEY&country=US&year=2019")
ResponseType("json")
LoadStrategy(Full)

TableDefinition({
  "uuid": "VARCHAR(255)",
  "name": "VARCHAR(255)",
  "date": "DATE",
  "observed": "DATE",
  "public": "BOOLEAN",
})

def Paginate(previousResponse):
  return None

def Transform(response):
  holidays = []
  for holiday in response['holidays']:
    holidays.append({
      "uuid": holiday['uuid'],
      "name": holiday['name'],
      "date": holiday['date'],
      "observed": holiday['observed'],
      "public": holiday['public'],
    })
  return holidays
```


All Port function names uses title-cases (e.g., "FunctionName"). You can also define helper functions
in the Port file. As a convention, we recommend using a lower-case first letter for helper
function names to easily distinguish helper function for Port configuration functions. Builtin helpers
provided by Port follow this part.

Environment Variables can be used in any string provided to a configuration functions using bash syntax
(e.g., `GET"https://holidayapi.com/v1/holidays?key=$HOLIDAY_API_KEY")`, `BasicAuth("user", "$PASSWORD")`)

# Starlark

For simplicity and long-term maintainability, Port files are written in the [Starlark](https://docs.bazel.build/versions/master/skylark/language.html) language, which is a Python dialect.

Basic funcitionality is similar to Python, but differs in that it does not have module importing, anonymous functions (lamdas), and a few [other differences](https://docs.bazel.build/versions/master/skylark/language.html#differences-with-python).

Read the complete [Starlark Spec](https://github.com/bazelbuild/starlark/blob/master/spec.md) to see all features built-in to the language.

# Configuration Functions

#### Get(url)

Get configures an API endpoint for extraction using the HTTP GET Verb.

Arguments:

* `url` - (string) the full URL of the API endpoint to be extracted. 

#### AddHeader(name, value)

AddHeader configures a header value to the HTTP request made to this API endpoint

Example:

```
AddHeader("Authorization": "Bearer $MY_API_BEARER_TOKEN")
```

Arguments:

* `name` - (string) the name of the HTTP Header
* `value` - (string) the value for the HTTP Header

#### BasicAuth(username, password)

BasicAuth declares that this API endpoint will use HTTP BasicAuth authentication.

Arguments:

* `username` - (string) the username for HTTP BasicAuth
* `password` - (string) the password for HTTP BasicAuth

#### ResponseType(format)

Configures the response type for the API endpoint, which is used to unmarshall the response body
before further processing.

Arguments:

* `format` - (string) name of the format. available options: `"json"`

#### LoadStrategy(type, \*\*options)

LoadStrategy configures how the destination table will be updated.

Available strategies:

* `Full` - complete replacement of the destination table with the extracted rows
* `Increment` - upsert or insert extracted rows based on `primary_key`

`options` by strategy:

| Type         | Options
| ------------ | ---------------------------------------------- |
| Full         | (none)                                         |
| Incremental  | primary_key                                    |

Examples:

```python
LoadStrategy(Full)
```

```python
LoadStrategy(Incremental, primary_key='id')
```

Arguments:

* `type` - (constant) one of 3 predeclared constants that are used without quotes: `Full`, `Incremental`
* `options` - (keyword-args) keyword arguments for configuration based on type

#### TableDefinition(columns)

TableDefinition configures the schema of the destination table.

Arguments:

* `columns' - (dict) a string/string dictionary where each key is the column name and each value is
  the data type. e.g., `{ "id": "BIGINT", "name": "VARCHAR(255)", "active": "BOOLEAN" }`

#### ErrorHandling(errorsToHandlingDict)

Error handling determines what to do when different types of errors occur.

Error Types (constants):

* `NetworkError` - An error occurred when attempting to make an HTTP request to the server
* `Http4XXError` - The server responded with a 4XX status code
* `Http5XXError` - The server responded with a 5XX status code
* `InvalidBodyError` - An error occurred while attempting to parse the response body using the
  configured response type

Handlers (constants):

* `Fail` - stop the job and report the error
* `Retry` - retry the current HTTP request

Example:

```
ErrorHandling({
  NetworkError: Retry,
  Http4XXError: Fail,
  Http5XXError: Retry,
  InvalidBodyError: Fail,
})
```

Arguments:

* errorsToHandlingDict - (dict constant/constant) a dictionary mapping Error Type constants to 
  Handler constants. e.g., `{ NetworkError: Retry, Http4XXError: Fail }`


#### def Paginate(previousResponse)

Paginate is a function that returns pagination values to be used in the next request. Paginate is
expected to return a string/string dict where each key is a parameter that will be interopolated (via
curly-brace enclosed tokens, e.g., `{key}`) into the request URL or Header and each value is the next
page's value for that parameter. Paginate must return None on the last page to stop further requests.

Paginate is called before the request for the first page with an argument of `None`. Use a conditional
statement to return the initial value when this happens.

Example configuration using pagination:

```python
Get("https://api.com/resource?offset={offset}")

def Paginate(previousResponse):
  if previousResponse == None: # Set values for First page
    return { 'offset': 0 }
  elif previous_response['body']['hasMore']: # Subsequent Pages
    return { 'offset': previous_response['body']['offset'] }
  else:
    return None # Indicates last page, do not iterate again
```

#### def Transform(response)

Transform is a function that transforms the marshalled response body into tabular data in the format
of an array of dicts.

You can use the Transform function to perform a variety of simple transforms:

* Filter rows
* Select columns to return
* Format numbers, dates or timestamps
* Flatten nested values in the response object
* Replace machine Enum values with the human form
* Unserialized and flatten serialized attributes

Helper functions can also be used in the Transform function.

Example transform using a user-defined helper (`to_date`) and a builtin helper (`dig`):

```
def to_date(value):
  if value == None or value == "":
    return None

  return time.fromtimestamp(int(value) // 1000).format("2006-01-02T15:04:05Z07:00")

def Transform(response):
  deals = []
  for deal in response['deals']:
    to_date([])
    deals.append({
      'deal_id': deal['dealId'],
      'deal_name': dig(deal, 'properties', 'dealname', 'value'),
      'close_date': to_date(dig(deal, 'properties', 'closedate', 'value')),
      'deal_stage': dig(deal, 'properties', 'dealstage', 'value'),
      'pipeline': dig(deal, 'properties', 'pipeline', 'value'),
      'create_date': to_date(dig(deal, 'properties', 'createdate', 'value')),
      'deal_source': dig(deal, 'properties', 'deal_source', 'value'),
      'associated_company_id': dig(deal, 'associations', 'associatedCompanyIds', 0),
    })

  return deals
```

# Builtin Helper Methods

#### dig(object, key1, key2, ....)

`dig` is used to repeatedly traverse a nested dict or list object. If any key is not found in the
object, the function halts and returns `None`

Arguments:

  * `object` - (list or dict) - an list or dict to traverse
  * `keys` - (variadic argument) - 1 or more keys or indices (in the case of lists) to fetch

Examples:

```
dict = { "a": { "b": [{ "c": 4 }] } }
list = [{"a": 9}]

dig(dict, "a", "b", 0, "c")) # 4
dig(list, 0, "a") # 9

dig(dict, "a", "b", 1, "c") # None
dig(list, 1) # None
```
