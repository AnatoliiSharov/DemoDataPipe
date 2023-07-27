FROM postgres:15.3

COPY db_scripts/init.sql /docker-entrypoint-initdb.d/init.sql
COPY db_scripts/create_tables.sql /docker-entrypoint-initdb.d/create_tables.sql
