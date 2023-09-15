FROM postgres:15.3-alpin

COPY ./create_table.sql /docker-entrypoint-initdb.d/create_table.sql

ENV POSTGRES_DB: crawler
ENV POSTGRES_USER: postgres
ENV POSTGRES_PASSWORD: 1111

EXPOSE 5432
