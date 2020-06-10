# Tutorial

## Create Your "Pad"

Generate a "pad" (what Teleport calls your project directory) with the new command

  $ teleport new path/to/pad


## Connect a Database

Configure a database by create a new config file in your preferred format (JSON, YAML, TOML, supported) with the name of this data source.  Example, add a new file "mydb.yml":

    databases/
      |-- mydb.yml

With contents:

    url: postgres://user:$PASSWORD@host.tld/production

Teleport automatically detects ENV variables and replaces any bash-style references to them in your configuration.

To verify the database connection, you can run the `teleport list-tables` command to connect and display the tables in that database:

    $ teleport list-tables -source mydb

## Connect a Data Warehouse

Next, let's configuration a datawarehouse in the same manner. For simplicity's sake we will use a local SQLite database for this example:

    databases/
      |-- mydb.yml
      |-- datawarehouse.yml

With contents:

    url: sqlite://datawarehouse.sqlite3

And verify this new database:

  $ teleport list-tables -source datawarehouse

This command will not output any results (but also not return an error) because the sqlite database did not exist before running this command. It will be automatically created.

## Transfer a table from the database to the Data Warehouse 

Now we're ready to run our first pipeline. Let's extract a table from the configured database and load it into the data warehouse (replace "{{Table}}" with the name of a table in your database).

    $ teleport extract-load -from mydb -to datawarehouse -table {{Table}} -preview

The "-preview" flag indicates we are running a dry-run (no data will be moved or schemas modified) with verbose logging enabled so you can see exactly what teleport will do before it does it. You should get an output like this:

    $ teleport extract-load -from mydb -to datawarehouse -table {{Table}} -preview
    INFO[0000] Starting extract-load                         from=mydb table=widgets to=datawarehouse
    DEBU[0000] Establish connection to Database              database=mydb
    DEBU[0000] Establish connection to Database              database=datawarehouse
    DEBU[0000] Inspecting Table                              database=mydb table=widgets
    INFO[0000] Destination Table does not exist, creating    database=datawarehouse table=mydb_widgets
    DEBU[0000] (not executed) SQL Query:
      CREATE TABLE mydb_widgets (
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
    DEBU[0000] Exporting CSV of table data                   database=mydb table=widgets
    DEBU[0000] Results CSV Generated                         file=/tmp/extract-widgets-mydb207658357 limit=3
    DEBU[0000] CSV Contents:
      Headers:
      id,name,description,price,quantity,active,launch_date,updated_at,created_at

      Body:
      *****


    DEBU[0000] Creating staging table                        database=datawarehouse staging_table=staging_mydb_widgets
    DEBU[0000] (not executed) SQL Query:
      CREATE TABLE staging_mydb_widgets AS SELECT * FROM mydb_widgets LIMIT 0
    DEBU[0000] (not executed) Importing CSV into staging table  database=datawarehouse staging_table=staging_mydb_widgets
    DEBU[0000] Promote staging table to primary              database=datawarehouse staging_table=staging_mydb_widgets table=mydb_widgets
    DEBU[0000] (not executed) SQL Query:

          ALTER TABLE mydb_widgets RENAME TO old_mydb_widgets;
          ALTER TABLE staging_mydb_widgets RENAME TO mydb_widgets;
          DROP TABLE old_mydb_widgets;

    WARN[0000] 0 rows processed
    INFO[0000] Completed extract-load 🎉                      from=mydb rows=0 table=widgets to=datawarehouse

A lot happened in our simple command! The extract-load job will automatically:

  1. Look for a destination table with name "{{source}}_{{table}}"
  2. Create that table if it does not exist, based on the schema from the extracted table
  3. If the table did exist, teleport would inspect its columns and compare to the extracted table to determine which columns are loadable
  4. Perform the extract: export a CSV of data from the extracted table
  5. Create a staging table in the load target database with the same schema as the destination table
  6. Import the CSV using the most performant method depending on the type of database
  7. In a single transaction, replace the current destination table with the staging table that contains updated data.

If we are happy with the plan and want to run the command for real, simply remove the -preview flag:

    $ teleport extract-load -from mydb -to datawarehouse -table {{Table}}
    INFO[0000] Starting extract-load                         from=mydb table=widgets to=datawarehouse
    INFO[0000] Destination Table does not exist, creating    database=datawarehouse table=mydb_widgets
    WARN[0000] 0 rows processed
    INFO[0000] Completed extract-load 🎉                      from=mydb rows=0 table=widgets to=datawarehouse

We have now implemented our first high-performance, data pipeline with only 2 lines of configuration and 1 command 💥

## Connect an API

Coming soon! For a sneak peek, see: TODO

## Next Steps

Teleport currently supports the following database:

Extracting:

  * MySQL
  * Postgres
  * SQLite

Loading:

  * Postgres
  * Redshift
  * SQLite

Run `teleport help` to see all available commands:

    TODO
