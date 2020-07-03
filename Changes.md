# Teleport Changelog

## HEAD

* (API) Helper method for formatting timestamps as strings using C-style strftime: `time.strftime()`

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

