# export AWS_REGION=
# export AWS_ACCESS_KEY_ID=
# export AWS_SECRET_ACCESS_KEY=
# export DATAWAREHOUSE_REDSHIFT_URL=

echo "Loading tables into the datawarehouse from multiple sources..."

ls config/connections/

teleport about-db -s appdb
teleport about-db -s crm
teleport about-db -s datawarhouse

teleport list-tables -s appdb
teleport list-tables -s crm
teleport list-tables -s datawarehouse

teleport load -s appdb -t users -d datawarehouse
teleport list-tables -s datawarhouse

teleport load -s crm -t customers -d datawarehouse
teleport list-tables -s datawarhouse

psql $DATAWAREHOUSE_REDSHIFT_URL

  SELECT * FROM appdb_users LIMIT 3;
  SELECT * FROM crm_customers LIMIT 3;

  \q 

echo "TADA 🎉"

echo "Using transforms to create composite report tables...."

ls transforms/
cat transforms/users_full_report.sql

teleport update-transform -s datawarhouse -t users_full_report

teleport list-tables -s datawarhouse

psql $DATAWAREHOUSE_REDSHIFT_URL

  SELECT * FROM users_full_report LIMIT 3;

  \q

echo "TADA 🎉"
