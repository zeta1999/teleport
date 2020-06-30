Each database connection is configured in its own file in the "databases/" folder.Database
configuration can use your preferred configuration format: YAML, JSON, TOML or EDN. The file extension
tells Teleport which configuration format was used.

For all examples in the wiki, YAML will be used.

# Adding a New Database connection

Create the database configuration file in your Pad:

```
$ cd path/to/pad
$ touch databases/new_database.yml
```

# Configuration Options

The database is configured via 2 keys:

  * `url` - (string) the database connection URL
  * `options` - (key/value, optional) adapter-specific database options

The `url` key has format: `{protocol}://{user}:{password}@{host}/{database_name}?{paramter1=value,parameter2=value...}`. Environment variables can be used in the string to pass configuration and protect secrets.

The `options` key has further key/value pairs to set adapter specific options.

## Supported Protocols

| Database   | Protocol  | Options
| ---------- | --------- | -----------------
| SQLite     | sqlite    | 
| MySQL      | mysql     | 
| PostgresQL | postgres  | 
| Redshift   | redshift  | s3_bucket, s3_region, service_role
| Snowflake  | snowflake | s3_bucket, external_stage_name

## Examples

### A Postgres configuration

```yaml
url: postgres://user:$PASSWORD@postgres.host/production
```

### A SQLite configuration

```yaml
url: sqlite://$PADPATH/tmp/db.sqlite3
```

The `$PADPATH` environment variable is used here to ensure the configuration points to a SQLite file
in the Pad's tmp/ directory when the `teleport` command is running from a different directory than the
current directory.

### A Redshift configuration

```
url: redshift://user:$REDSHIFT_PASSWORD@$REDSHIFT_ENDPOINT/dbname
options:
  s3_bucket: mys3bucket
  s3_region: us-east-1
  service_role: arn:aws:iam::account-id:role/redshift-copy-role

```
