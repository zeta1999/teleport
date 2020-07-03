# Teleport

One framework for all your data ingestion needs

Teleport's ambition is to become the standard for building ELT data ingestion pipelines. It provides an opionated, convention-over-configuration framework to allow you to pack your data warehouse, hydrate your data lake, or add a drop to your data pond from any or all of your 1st party and 3rd party data services.

Following the guidelines of ELT, Teleport does not provide support for complex, arbitrary data workflows. Instead, Teleport serves to provide just enough tooling and standardization to get all your data where it needs to go with the "EL" (extract-load) steps and moves all the complexity of preparing your data for business use to the "T" (transform) step.

Teleport is currently in "alpha" testing. Please give it a try and report any all bugs by creating an issue. 

See the ["Contributing"](#Contributing) section for how to get involved in Teleport's development.

# Features

* Manage all data source and ELT pipeline configurations in version control
* Extract data from relational databases
* Extract data from APIs
* Parse API responses into a tabular data structure
* Load data into relational databases or data warehouses
* Detailed logging for monitoring and debugging ELT pipelines
* SQL statements to transform raw data tables into report tables
* All commands available in a single Command Line Interface

# Installation (alpha)

Install the `teleport` binary on:

## Mac or Linux

One step install:

```
curl -fsSL  https://teleport-releases.s3.amazonaws.com/latest/install.sh | bash
```

## Linux Packages

Teleport RPM/DEB packages for any 64-bit Linux OS are available:

```
# DEB distros like Ubuntu
curl -fsSL  https://teleport-releases.s3.amazonaws.com/v0.0.1-alpha.1/teleport_0.0.1-alpha.1_amd64.deb
dpkg -i teleport_0.0.1-alpha.1_amd64.deb

# RPM distros like CentOS
curl -fsSL  https://teleport-releases.s3.amazonaws.com/v0.0.1-alpha.1/teleport_0.0.1_alpha.1_x86_64.rpm
yum install teleport_0.0.1_alpha.1_x86_64.rpm
```

## Docker

1. Download the [Dockerfile](https://raw.githubusercontent.com/teleport-data/teleport/master/Dockerfile) to your local Pad
2. Build the container: `docker build -t teleport`
3. Run the container: `docker run -t teleport  -e <ENV Variables> -v $(pwd):/pad [COMMAD] [OPTIONS]`

## From Source

See the [Development wiki page](https://github.com/Teleport-Data/teleport/blob/master/wiki/development.md) for instructions on how to check out the source and build it yourself.

# Usage

Create a new "Pad" (Teleport's term for project directory) with and then cd to the created directory:

    $ teleport new pad-name
    $ cd pad-name

<details><summary>To see all Teleport commands, run `teleport help`</summary>

    $ teleport help
    Commands:
      new <path/to/pad>	generate a new pad folder at the given path
      help			show this message
      version		print version information

      extract		export all data from a database table to CSV. Required options: -from, -table
      extract-api		export all data from an API endpoint to CSV. Required options: -from, -endpoint

      extract-load		extract all data from a table in one database to another database. Required options: -from, -to, -table
      extract-load-api		extract all data from an API endpoint to a database. Required options: -from, -to, -endpoint

      transform		(re-)generate a materialized table form a sql statement. Required options: -source, -table

      about-db		show connection information for a database. Required options: -source
      db-terminal		start a terminal for interacting with a database. Required options: -source
      list-tables		list the tables in a database. Required options: -source
      drop-table		drop a table. Required options: -source, -table
      describe-table	print the schema for a table. Required options: -source, -table

    Options:
      -source, -s [source]	data source name
      -from [source]	data source to extract data from
      -to [source]		data source to load data into
      -table, -t [table]	name of table in the database data source
      -endpoint, -e [table]	name of endpoint in the API data source
      -preview, -p		preview command as a dry-run without making any changes
      -debug, -d		enable debug log output
</details>

# Pad Structure

Pads have this directory structure:
    
    pad-name/
      |- apis/
        |- exampleapi1.port
        |- exampleapi2.port
        |- ...
      |- databases/
        |- exampledb1.yml
        |- exampledb2.yml
        |- ...
      |- transforms/
        |- exampletrasnform1.sql
        |- exampletransform2.sql
        |- ...

When refering to a resource (data source or transform) in a Teleport command, the name of the resource is the filename without the extension. e.g., to list the tables for the database defined in `databases/exampledb1.yml`, use `teleport list-tables -source exampledb1`

For API configurations, Teleport uses its own "Port" configuration language. "Port" is a declarative, Python dialect
used for configuration and mapping data. For full documentation on the "Port" configuration language, [visit the wiki](https://github.com/Teleport-Data/teleport/blob/master/wiki/api_configuration.md)

<details><summary>Example "Port" file for the [Holiday API](https://holidayapi.com/docs)</summary>

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

def Paginate(previous_response):
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
</details>


For Database configurations, Teleport supports the following file formats: YAML, JSON, TOML, EDN. For full documentation on database configuration, [visit the wiki](https://github.com/Teleport-Data/teleport/blob/master/wiki/database_configuration.md)

For Transforms, Teleport supports SQL statements and that create a table named based on the filename without extension. To update a transform table, use `teleport transform -source <source> -table <table>`

# Deployment

Coming soon...

# Contributing

All contributions are welcome! To get invovled:

* Open an issue with either a bug report or feature request
* Verify existing bug reports and adding reproduction steps
* Review Pull Requests and test changes locally on your machine
* Writing or Editing Documentation

Newbies welcome! Feel free to reach out to a maintainer for help submitting your first Pull Request.

# Teleport Pro

Teleport is funded by the Teleport Pro commercial offering. Teleport Pro is an extension to Teleport that includes:

* More Features
* A Commercial License
* Priority Support
* Allows you to support further development of open source Teleport

More details coming soon...
