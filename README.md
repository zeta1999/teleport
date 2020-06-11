# Teleport

One framework for all your data ingestion needs

Teleport's ambition is to become the standard for building ELT data ingestion pipelines. It provides an opionated, convention-over-configuration framework to allow you to pack your data warehouse, hydrate your data lake, or add a drop to your data pond from any or all of your 1st party and 3rd party data services.

Following the guidelines of ELT, Teleport does not provide support for complex, arbitrary data workflows. Instead, Teleport serves to provide just enough tooling and standardization to get all your data where it needs to go with the "EL" (extract-load) steps and moves all the complexity of preparing your data for business use to the "T" (transform) step.

Teleport is currently in "alpha" testing. Please give it a try and report any all bugs by creating an issue. 

See the ["Contributing"](#Contributing) section for how to get involved in Teleport's development.

# Installation

Install Teleport via

* Homebrew
* deb
* rpm
* build
* Docker

Details coming soon...

# Usage

Create a new "Pad" (Teleport's term for project directory) with and then cd to the created directory:

    $ teleport new pad-name
    $ cd pad-name

To see all Teleport commands, run:

    $ teleport help
    usage: teleport [COMMAND] [OPTIONS]
    Commands:
      new <path/to/pad>	generate a new pad folder at the given path
      help			show this message
      version		print version information

      extract		export all data from a database table to CSV. Required options: -from, -table
      extract-api		export all data from an API endpoint to CSV. Required options: -from, -endpoint

      extract-load		export all data from a table in one database to another. Required options: -from, -to, -table
      extract-load-api		export all data from an API endpoint to a database. Required options: -from, -to, -endpoint

      transform		(re-)generate a materialized table form a sql statement. Required options: -source, -table

      about-db		show connection information a database. Required options: -source
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

# Concepts

* **API** - A data source that is accessed via an HTTP API. Can be internal or 3rd party.
* **Data Source** - Anything that stores data to which Teleport can connect for extracting and/or loading. Currently, Teleport supports 2 kinds of data sources: APIs and Databases. Is referred to as "source" in command line arguments.
* **Database** - A relational database that is accessed via SQL.
* **Endpoint** - An endpoint is a specific HTTP path within an API to fetch a certain type of resource. e.g., An API to a CRM will likely have endpoints for contacts, companies, and deals.
* **Parser** - A script that parses the response from an API and transforms it into a flat Dict object that can be easily loaded into a database table. Teleport parsers are scripts written in [Starlark](https://github.com/bazelbuild/starlark/blob/master/spec.md). Don't be scared about a new language! If you know Python, Starlark is basically a subset of Python with a smaller standard library and no modules. For a full list of differences, see [Differences with Python](https://docs.bazel.build/versions/master/skylark/language.html#differences-with-python).
* **Transforms** - Transforms are the "T" in ELT. These are SQL statements that generate a new table based on raw input tables.

# Pad Structure

Pads have this directory structure:
    
    pad-name/
      |- apis/
        |- exampleapi1.yml
        |- exampleapi2.yml
        ....
        |- parsers/
          |- exampleapi1/parse_body.star
          |- exampleapi2/parse_body.star
      |- databases/
        |- exampledb1.yml
        |- exampledb2.yml
        ....
      |- transforms/
        |- exampletrasnform1.sql
        |- exampletransform2.sql
        ....

While the examples here are all ".yml" configuration files, Teleport supports the following formats: YAML, JSON, TOML, EDN

When refering to a resource in a Teleport command, the name of the resource is the filename without the extension. e.g., to list the tables for the database defined in `databases/exampledb1.yml`, use `teleport list-tables -source exampledb1`

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