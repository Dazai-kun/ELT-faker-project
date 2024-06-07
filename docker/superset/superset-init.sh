#!/bin/bash
#Exit immediately if a command exits with a non-zero status.
set -e 


#Create the duckDB database file
python -c "
import duckdb
conn = duckdb.connect('/app/superset_home/db/duckdb_dw.duckdb')
conn.close()
"


# create Admin user, you can read these values from env or anywhere else possible
superset fab create-admin --username "$ADMIN_USERNAME" --firstname Superset --lastname Admin --email "$ADMIN_EMAIL" --password "$ADMIN_PASSWORD"

# Upgrading Superset metastore
superset db upgrade

# setup roles and permissions
superset superset init 


superset set_database_uri -d DW -u duckdb:///superset_home/db/duckdb_dw.duckdb


# Starting server
/bin/sh -c /usr/bin/run-server.sh