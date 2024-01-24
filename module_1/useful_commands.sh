#!/bin/bash

# directory for postgres data persistance
mkdir -p ny_taxi_postgres_data

# run postgres with docker
docker run -d \
	-e POSTGRES_USER="root" \
	-e POSTGRES_PASSWORD="root" \
	-e POSTGRES_DB="ny_taxi" \
	-v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
	-p 5432:5432 \
	postgres

# connect to postgres db using pgcli
pgcli -h localhost -p 5432 -u root -d ny_taxi
