# Teleport Changelog

## HEAD
### Added
* (Database) Progress indication in DEBUG log for database extracts
* Plugins system for packaging deployment options
* New configuration methods `Paginate(function/lambda)` and `Transform(function/lambda)` for APIs
* Support for the Snowflake database (https://www.snowflake.com)

### Deprecated
* API Configuration functions: `def Paginate():` and `def Transform():` (new methods preferred)

## 0.0.1-alpha.6

* Configure the job schedule for teleport commands in `config/schedule.port`
* `teleport schedule` CLI commands to validate and export the job schedule

## 0.0.1-alpha.5

* (Database) Support TLS for Amazon RDS MySQL instances
* Support '.port.py' extension for Port configuration files to simplify enabling IDE syntax highlight
* (Database) Add optional 3rd argument "column_type" to TransformColumn()
* (Database) -full forces the Full load strategy to be used (overriding the configured strategy) for batch operations
* (Database) Schema connection configuration option

## 0.0.1-alpha.4

* Disable secrets initialization in `teleport new` (for now until secret key dependence is fixed)

## 0.0.1-alpha.3

* New pad directory structure: `apis/` and `databases/` folders now live under `sources/` and database
  connection settings have been moved to a single file: `config/databases.yml`

## 0.0.1-alpha.2

* (API) New Response Type `"csv"` for parsing a CSV to table format
* (API) Helper method for formatting timestamps as strings using C-style strftime: `time.strftime()`
* (Database) Configuration language for extracting from databases
* (Database) ComputeColumn() and TransformColumn() configuration methods
* CLI command `teleport secrets` to manage storing encrypted ENV secrets

## 0.0.1-alpha.1

Initial Release:

- In-database "Transform" step via SQL statements
- "Pad" directory structure
- `teleport` CLI
- Installation for Mac and Linux
- Dockerfile for building Docker image
- Preview mode: preforms a dry-run with verbose logging

#### Databases (Extract and Load)

- Extract from a Database (MySQL, Postgres, Redshift, SQLite supported)
- Load to a  Database (Postgres, Redshift, SQLite supported)
- Extract-Load table from one Database to another Database 
  - Table Name Convention (`{{extracted_db}}_{{table_name}}`)
  - Automatically create destination table if it does not exist 
  - Determine load-able columns by comparing source and destination columns
  - Load Strategies: Full, Incremental, Modified-Only

#### APIs (Extract and Load)

- Configure APIs using "Port" python-ic configuration DSL for:
  - URL
  - Authentication
  - Response Type
  - Pagination
  - Load Strategy
  - Transform
- Table Name Conventions (file name without extension)

