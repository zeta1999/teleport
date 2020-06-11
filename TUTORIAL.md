# Tutorial

How to create and configure your first "Pad" (what Teleport calls your project directory) via the CLI.

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

Coming soon! For a sneak peek, see: TODO