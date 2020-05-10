# export AWS_REGION=
# export AWS_ACCESS_KEY_ID=
# export AWS_SECRET_ACCESS_KEY=
# export DATAWAREHOUSE_REDSHIFT_URL=
# teleport drop-table -s datawarehouse -t appdb_users
# teleport drop-table -s datawarehouse -t crm_companies
# teleport drop-table -s datawarehouse -t users_full_report

teleport help

echo "Loading tables into the datawarehouse from multiple sources..."

ls config/connections/

teleport about-db -s appdb
teleport about-db -s crm
teleport about-db -s datawarhouse

teleport extract-load -from appdb -to datawarehouse -table users
teleport list-tables -s datawarhouse

teleport extract-load -from crm -to datawarehouse -table companies
teleport list-tables -s datawarhouse

psql $DATAWAREHOUSE_REDSHIFT_URL

  SELECT * FROM appdb_users LIMIT 3;
  SELECT * FROM crm_companies LIMIT 3;

  \q 

echo "TADA 🎉"

echo "Using transforms to create composite report tables...."

ls transforms/
cat transforms/users_full_report.sql

teleport transform -source datawarhouse -table users_full_report

teleport list-tables -s datawarhouse

psql $DATAWAREHOUSE_REDSHIFT_URL

  SELECT * FROM users_full_report LIMIT 3;

  \q

echo "TADA 🎉"
