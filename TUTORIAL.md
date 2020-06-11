# Tutorial

How to create and configure your first "Pad" (what Teleport calls your project directory) via the CLI.

## Contents

* [Create Your Pad](#Create_Your_Pad)
* [Connect a Database](#Connect_a_Database)
* [Connect a Data Warehouse](#Connect_a_Data_Warehouse)
* [Transfer a table from the database to the Data Warehouse](#Transfer_a_table_from_the_database_to_the_Data_Warehouse)
* [Connect an API](#Connect_an_API)
* [Extract an API endpoint to a Data Warehouse table](#TODO)


## Create Your Pad

Generate a new Pad with the new command:

  $ teleport new teleport-tutorial

Change to the pad directory:

  $ cd teleport-tutorial

Setup git version control:

    $ git init
    $ git add .
    $ git commit -m "Generated a new Teleport pad"

## Connect a Database

This example assumes you have a PostgreSQL database available for testing on your local machine. We will create a new table "widgets" in that database with test data.

To configure the connection information for your PostgreSQL database, create a new YAML config file (you can also use JSON, TOML, or EDN, but we will use YAML for this tutorial) with the name "local".

    $ touch databases/local.yml

The configuration file needs one item: the database URL. To specify it, edit `databases/local.yml` with your preferred editor to add the following contents:

    url: postgres://localhost/$DBNAME?sslmode=disable

Notice we have used the `$DBNAME` environment variable in the URL for the database name. When running a Teleport command, you will need to provide this environment variable to Teleport. For this tutorial, we will use the `export` command in the current shell:

    $ export DBNAME="..."

Replace "..." with the actual database name value.

Now let's verify the database connection by using the `teleport list-tables` command to connect and display the tables in that database:

    $ teleport list-tables -source local

If you are using a brand new database with no tables yet, you will see no output. If there was a problem, you will see an error.

To load the sample data, we will use some internal teleport commands whose explanation is outside the scope of this tutorial:

    $ curl -o tmp/example_widgets.yml https://raw.githubusercontent.com/hundredwatt/teleport/master/test/example_widgets.yaml
    $ curl -o tmp/example_widgets.csv https://raw.githubusercontent.com/hundredwatt/teleport/master/test/example_widgets.csv
    $ teleport create-destination-table-from-config-file -source local -file test/example_widgets.yaml
    $ teleport import-csv -source local -table example_widgets -file test/example_widgets.csv

You can now verify the sample data was loaded by accessing psql with the `db-terminal` command:

    $ teleport db-terminal -source local
    psql (12.2, server 12.3)
    Type "help" for help.

    dbname=# SELECT * FROM example_widgets;
    id |  name  |       description       | price | quantity | active | launch_date |     updated_at      |     created_at
    ----+--------+-------------------------+-------+----------+--------+-------------+---------------------+---------------------
      1 | Fidget | The best fidget         |  9.99 |       10 | t      | 2010-05-01  | 2010-05-01 14:22:00 | 2010-04-15 14:22:00
      2 | Jabit  | An amazing jabit fidget | 19.99 |       20 | f      | 2011-08-01  | 2011-07-01 14:22:00 | 2011-07-01 14:22:00
      3 | Tonnit | Our best tonnit         | 29.99 |       14 | t      | 2014-02-01  | 2013-12-21 19:22:00 | 2014-02-15 14:22:00
    (3 rows)

    dbname=#

Commit your configuration changes and then continue to the next section.

## Connect a Data Warehouse

Next, let's configuration a datawarehouse in the same manner. For simplicity's sake we will use a local SQLite database for this example.

Create a new configuration file for the datawarehouse:

    $ touch databases/datawarehouse.yml

And add the contents with your favorite editor:

    url: sqlite://$PADPATH/tmp/datawarehouse.sqlite3

The $PADPATH environment variable contains the path to your Pad. So this configuration will create a new SQLite database in your Pad's `tmp/` directory.

And verify this new database:

  $ teleport list-tables -source datawarehouse

If no errors are reported, then the database is configured correctly. There will be no other output because this is a newly created database with no tables yet.

Commit your configuration changes and then continue to the next section.

## Transfer a table from the database to the Data Warehouse 

Now we're ready to run our first pipeline. Let's extract a table from the `local` database and load it into the `datawarehouse` database.

    $ teleport extract-load -from local -to datawarehouse -table example_widgets -preview

The "-preview" flag indicates we are running a dry-run (no data will be moved or schemas modified) with verbose logging enabled so you can see exactly what teleport will do before it does it. You should get an output like this:

    $ teleport extract-load -from local -to datawarehouse -table example_widgets -preview
    INFO[0000] Starting extract-load                         from=local table=example_widgets to=datawarehouse
    DEBU[0000] Establish connection to Database              database=local
    DEBU[0000] Establish connection to Database              database=datawarehouse
    DEBU[0000] Inspecting Table                              database=local table=example_widgets
    INFO[0000] Destination Table does not exist, creating    database=datawarehouse table=local_example_widgets
    DEBU[0000] (not executed) SQL Query:
      CREATE TABLE local_example_widgets (
      id INT8,
      name VARCHAR(255),
      description VARCHAR(65536),
      price DECIMAL(10,2),
      quantity INT8,
      active BOOLEAN,
      launch_date DATE,
      updated_at TIMESTAMP,
      created_at TIMESTAMP
      );
    DEBU[0000] Exporting CSV of table data                   database=local table=example_widgets
    DEBU[0000] Results CSV Generated                         file=/tmp/extract-example_widgets-local940716871 limit=3
    DEBU[0000] CSV Contents:
      Headers:
      id,name,description,price,quantity,active,launch_date,updated_at,created_at

      Body:
      1,Fidget,The best fidget,9.99,10,true,2010-05-01 00:00:00,2010-05-01 14:22:00,2010-04-15 14:22:00
      2,Jabit,An amazing jabit fidget,19.99,20,false,2011-08-01 00:00:00,2011-07-01 14:22:00,2011-07-01 14:22:00
      3,Tonnit,Our best tonnit,29.99,14,true,2014-02-01 00:00:00,2013-12-21 19:22:00,2014-02-15 14:22:00


    DEBU[0000] Creating staging table                        database=datawarehouse staging_table=staging_local_example_widgets
    DEBU[0000] (not executed) SQL Query:
      CREATE TABLE staging_local_example_widgets AS SELECT * FROM local_example_widgets LIMIT 0
    DEBU[0000] (not executed) Importing CSV into staging table  database=datawarehouse staging_table=staging_local_example_widgets
    DEBU[0000] Promote staging table to primary              database=datawarehouse staging_table=staging_local_example_widgets table=local_example_widgets
    DEBU[0000] (not executed) SQL Query:

          ALTER TABLE local_example_widgets RENAME TO old_local_example_widgets;
          ALTER TABLE staging_local_example_widgets RENAME TO local_example_widgets;
          DROP TABLE old_local_example_widgets;

    INFO[0000] Completed extract-load 🎉                      from=local rows=3 table=example_widgets to=datawarehouse

A lot happened in this simple command! The extract-load job will automatically:

  1. Look for a destination table with name "{{source}}_{{table}}"
  2. Create that table if it does not exist, based on the schema from the extracted table
  3. If the table did exist, teleport would inspect its columns and compare to the extracted table to determine which columns are loadable
  4. Perform the extract: export a CSV of data from the extracted table
  5. Create a staging table in the load target database with the same schema as the destination table
  6. Import the CSV using the most performant method depending on the type of database
  7. In a single transaction, replace the current destination table with the staging table that contains updated data.

If we are happy with the plan and want to run the command for real, simply remove the -preview flag:

    $ teleport extract-load -from local -to datawarehouse -table example_widgets
    INFO[0000] Starting extract-load                         from=local table=example_widgets to=datawarehouse
    INFO[0000] Destination Table does not exist, creating    database=datawarehouse table=local_example_widgets
    INFO[0000] Completed extract-load 🎉                      from=local rows=3 table=example_widgets to=datawarehouse

You can verify the data made it to the data warehouse by opening the SQLite database with the `sqlite3` command (Sadly, Teleport's db-terminal does not yet support SQLite... but it's on our roadmap!)

    $ sqlite3 tmp/datawarehouse.sqlite3
    SQLite version 3.24.0 2018-06-04 14:10:15
    Enter ".help" for usage hints.
    sqlite> SELECT * FROM local_example_widgets;
    1|Fidget|The best fidget|9.99|10|true|2010-05-01 00:00:00|2010-05-01 14:22:00|2010-04-15 14:22:00
    2|Jabit|An amazing jabit fidget|19.99|20|false|2011-08-01 00:00:00|2011-07-01 14:22:00|2011-07-01 14:22:00
    3|Tonnit|Our best tonnit|29.99|14|true|2014-02-01 00:00:00|2013-12-21 19:22:00|2014-02-15 14:22:00

We have now implemented our first high-performance, data pipeline with only 2 lines of configuration and 1 command 💥

## Connect an API

Now, we are going to configure a 3rd party API for extraction. For this tutorial, we will use the Holiday API: https://holidayapi.com. The Holiday API has 3 endpoints: Holidays, Languages and Countries and the documentation is available here: https://holidayapi.com/docs. 

We are going to create a Teleport configuration for this API.

First, create a new configuration file for this API:

    $ touch apis/holidayapi.yml

Now open it with your favorite editor and add contents:

    base_url: https://holidayapi.com/
    query_string:
      key: $HOLIDAY_API_KEY
    endpoints:
      2019_us_holidays:
        method: GET
        url: v1/holidays
        query_string:
          country: US
          year: 2019
        response_type: json
      countries:
        method: GET
        url: v1/countries
        response_type: json
      languages:
        method: GET
        url: v1/languages
        response_type: json

Holiday API only offers last year's holidays to free users, so we've configured the first endpoint as the 2019 US Holidays.

We also need to register for an API key at https://holidayapi.com/signup. Once we have an API key, set the $HOLIDAY_API_KEY environment variable:

    $ export HOLIDAY_API_KEY="..."

Replace "..." with the actual api key.

Now we are ready to extract data from this API. As a test, let's export to CSV:

    $ teleport extract-api -from holidayapi -endpoint 2019_us_holidays
    INFO[0000] Starting extract-api                          endpoint=2019_us_holidays from=holidayapi
    INFO[0000] Extract to CSV completed 🎉                    file=/tmp/extract-api-holidayapi-2019_us_holidays418193424 rows=1

Only 1 row, that's strange. We know there is more than 1 holiday per year :). Let's see what's going on:

    $ cat /tmp/extract-api-holidayapi-2019_us_holidays418193424
    ,,200,"These results do not include state and province holidays. For more information, please visit https://holidayapi.com/docs"

That result does not look like a Holiday at all. Let's look at the full response to see what's happening (I pulled this from my browser):

    {
        "status": 200,
        "warning": "These results do not include state and province holidays. For more information, please visit https:\/\/holidayapi.com\/docs",
        "requests": {
            "used": 29,
            "available": 9971,
            "resets": "2020-07-01 00:00:00"
        },
        "holidays": [
            {
                "name": "New Year's Day",
                "date": "2019-01-01",
                "observed": "2019-01-01",
                "public": true,
                "country": "US",
                "uuid": "82f78b8a-019e-479e-a19f-99040275f9bf",
                "weekday": {
                    "date": {
                        "name": "Tuesday",
                        "numeric": "2"
                    },
                    "observed": {
                        "name": "Tuesday",
                        "numeric": "2"
                    }
                }
            },
    ...

This endpoint return a lot more than just Holidays! If you carefully compare the 1 row CSV output to the full JSON response, you will notice that Teleport is only looking at the first level of keys and ignoring nested data structures. Teleport expects the result from an API endpoint to be a list of flat Dict objects so it can easily convert the result to a tabular format. To parse the raw response into the expected format, we will need to write our first Parser.

To get started with a Parser, let's create a script file for it:

    $ mkdir apis/parsers/holidayapi/
    $ touch apis/parsers/holidayapi/parse_holidays.star

Parsers are written using Starlark, a minimalist dialect of Python that allows us to quickly write parse scripts while maintaining performance no matter how much data our pipeline is processing. Read the [Starlark Spec](https://github.com/bazelbuild/starlark/blob/master/spec.md) to see everything Starlark can do. If you know Python, you will pick up Starlark in no time. Each Parser in Teleport defines a single function, `parse` that takes a single argument, which is the response body.

Back to the `2019_us_holidays` endpoint, we want to transform the full JSON response object into a list of flat Dict objects with each having the data for 1 holiday. Let's open `apis/parsers/holidayapi/parse_body.star` in our favorite editor and create that script:


    def parse(response_body):
      holidays = []
      for holiday in response_body["holidays"]:
        holidays.append({
          "uuid": holiday["uuid"],
          "name": holiday["name"],
          "date": holiday["date"],
          "observed": holiday["observed"],
          "public": holiday["public"],
          "country": holiday["country"],
          "weekday-date-name": holiday["weekday"]["date"]["name"],
          "weekday-date-numeric": holiday["weekday"]["date"]["numeric"],
          "weekday-observed-name": holiday["weekday"]["observed"]["name"],
          "weekday-observed-numeric": holiday["weekday"]["observed"]["numeric"]
        })

      return holidays

And update the configuration for the `2019_us_holidays` endpoint in `apis/holidayapi.yml` to use this Parser:

    ...
    endpoints:
      2019_us_holidays:
        method: GET
        url: v1/holidays
        query_string:
          country: US
          year: 2019
        response_type: json
        parsers:
          - holidayapi/parse_holidays.star
      countries:
    ....

Now let's re-run the `extract-api` command now that our new Parser is enabled:

    $ teleport extract-api -from holidayapi -endpoint 2019_us_holidays
    INFO[0000] Starting extract-api                          endpoint=2019_us_holidays from=holidayapi
    INFO[0000] Extract to CSV completed 🎉                    file=/tmp/extract-api-holidayapi-2019_us_holidays200396145 rows=146

146 rows this time! Let's see what they look like:

    $ head -n 3 /tmp/extract-api-holidayapi-2019_us_holidays200396145
    2019-01-01,true,US,2,Tuesday,82f78b8a-019e-479e-a19f-99040275f9bf,New Year's Day,2019-01-01,Tuesday,2
    2019-01-01,false,US,2,Tuesday,0e766ff3-0d31-40e1-85e1-49ed61ab006d,Seventh Day of Kwanzaa,2019-01-01,Tuesday,2
    2019-01-06,false,US,7,Sunday,61d8d9ba-8ce1-4e64-9b1a-f40d30f74a57,Epiphany,2019-01-06,Sunday,7

Each row is a Holiday, as we expected!

## Extract an API endpoint to a Data Warehouse table

Since we can't get an easy schema dump when extracting an API like we can for a database table, we will need to manually create the data warehouse table we want to load with the `2019_us_holidays` data.

To do this, let's re-run the `extract-api` command in `-preview` mode to see the CSV header row:

    $ teleport extract-api -from holidayapi -endpoint 2019_us_holidays -preview
    INFO[0000] Starting extract-api                          endpoint=2019_us_holidays from=holidayapi
    DEBU[0000] Requesting page                               page=0
    DEBU[0000] Applying transform                            transform=holidayapi/parse_holidays.star
    DEBU[0000] (preview) Skipping additional pages if any
    DEBU[0000] Results CSV Generated                         file=/tmp/extract-api-holidayapi-2019_us_holidays627218240 limit=3
    DEBU[0000] CSV Contents:
      Headers:
      name,date,public,weekday-date-name,weekday-date-numeric,weekday-observed-name,uuid,observed,country,weekday-observed-numeric

      Body:
      New Year's Day,2019-01-01,true,Tuesday,2,Tuesday,82f78b8a-019e-479e-a19f-99040275f9bf,2019-01-01,US,2
      Seventh Day of Kwanzaa,2019-01-01,false,Tuesday,2,Tuesday,0e766ff3-0d31-40e1-85e1-49ed61ab006d,2019-01-01,US,2
      Epiphany,2019-01-06,false,Sunday,7,Sunday,61d8d9ba-8ce1-4e64-9b1a-f40d30f74a57,2019-01-06,US,7


    INFO[0000] Extract to CSV completed 🎉                    file=/tmp/extract-api-holidayapi-2019_us_holidays627218240 rows=146

As expected, the names in the Header row match the object we created in our Parser. 

Now, we will need to manually create the table in our data warehouse (schema management for APIs is on Teleport's roadmap though!). Teleport's convention for the name of a data warehouse table loaded from an API endpoint is `{{api}}_{{endpoint}}`. So Teleport expects a table from this API named: `holidayapi_2019_us_holidays`. 

Let's create that table via the `sqlite3` command line tool:

    $ sqlite3 tmp/datawarehouse.sqlite3
    SQLite version 3.24.0 2018-06-04 14:10:15
    Enter ".help" for usage hints.
    sqlite> CREATE TABLE holidayapi_2019_us_holidays (
      uuid varchar(32),
      name varchar(255),
      date DATE,
      observed DATE,
      public boolean,
      country varchar(8),
      `weekday-date-name` varchar(8),
      `weekday-date-numeric` int,
      `weekday-observed-name` varchar(8),
      `weekday-observed-numeric` int
    );

Now we can use the `extract-load-api` command to extract data from the `2019_us_holidays` endpoint to the `holidayapi_2019_us_holidays` table in our data warehouse. Once again, let's use preview mode first:

    $ teleport extract-load-api -from holidayapi -endpoint 2019_us_holidays -to datawarehouse -preview
    INFO[0000] Starting extract-load-api                     endpoint=2019_us_holidays from=holidayapi to=datawarehouse
    DEBU[0000] Establish connection to Database              database=datawarehouse
    DEBU[0000] Inspecting Table                              database=datawarehouse table=holidayapi_2019_us_holidays
    DEBU[0000] Requesting page                               page=0
    DEBU[0000] Applying parser                               parser=holidayapi/parse_holidays.star
    DEBU[0000] (preview) Skipping additional pages if any
    DEBU[0000] Results CSV Generated                         file=/tmp/extract-api-holidayapi-2019_us_holidays503762773 limit=3
    DEBU[0000] CSV Contents:
      Headers:
      uuid,name,date,observed,public,country,weekday-date-name,weekday-date-numeric,weekday-observed-name,weekday-observed-numeric

      Body:
      82f78b8a-019e-479e-a19f-99040275f9bf,New Year's Day,2019-01-01,2019-01-01,true,US,Tuesday,2,Tuesday,2
      0e766ff3-0d31-40e1-85e1-49ed61ab006d,Seventh Day of Kwanzaa,2019-01-01,2019-01-01,false,US,Tuesday,2,Tuesday,2
      61d8d9ba-8ce1-4e64-9b1a-f40d30f74a57,Epiphany,2019-01-06,2019-01-06,false,US,Sunday,7,Sunday,7


    DEBU[0000] Creating staging table                        database=datawarehouse staging_table=staging_holidayapi_2019_us_holidays
    DEBU[0000] (not executed) SQL Query:
      CREATE TABLE staging_holidayapi_2019_us_holidays AS SELECT * FROM holidayapi_2019_us_holidays LIMIT 0
    DEBU[0000] (not executed) Importing CSV into staging table  database=datawarehouse staging_table=staging_holidayapi_2019_us_holidays
    DEBU[0000] Promote staging table to primary              database=datawarehouse staging_table=staging_holidayapi_2019_us_holidays table=holidayapi_2019_us_holidays
    DEBU[0000] (not executed) SQL Query:

          ALTER TABLE holidayapi_2019_us_holidays RENAME TO old_holidayapi_2019_us_holidays;
          ALTER TABLE staging_holidayapi_2019_us_holidays RENAME TO holidayapi_2019_us_holidays;
          DROP TABLE old_holidayapi_2019_us_holidays;

    INFO[0000] Completed extract-load-api 🎉                  endpoint=2019_us_holidays from=holidayapi rows=146 to=datawarehouse

Teleport follows a similar to process to how database tables are loaded. Looks like everything checked out, now let's run the command for real:

    $ extract-load-api -from holidayapi -endpoint 2019_us_holidays -to datawarehouse
    INFO[0000]O Starting extract-load-api                     endpoint=2019_us_holidays from=holidayapi to=datawarehouse
    INFO[0000] Completed extract-load-api 🎉                  endpoint=2019_us_holidays from=holidayapi rows=146 to=datawarehouse

And checkout the results:

    $ sqlite3 tmp/datawarehouse.sqlite3
    SQLite version 3.24.0 2018-06-04 14:10:15
    Enter ".help" for usage hints.
    sqlite> SELECT * FROM holidayapi_2019_us_holidays LIMIT 3;
    82f78b8a-019e-479e-a19f-99040275f9bf|New Year's Day|2019-01-01|2019-01-01|true|US|Tuesday|2|Tuesday|2
    0e766ff3-0d31-40e1-85e1-49ed61ab006d|Seventh Day of Kwanzaa|2019-01-01|2019-01-01|false|US|Tuesday|2|Tuesday|2
    61d8d9ba-8ce1-4e64-9b1a-f40d30f74a57|Epiphany|2019-01-06|2019-01-06|false|US|Sunday|7|Sunday|7

The holidays are now in our data warehouse as expected!