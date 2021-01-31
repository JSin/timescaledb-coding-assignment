#!/bin/bash
set -e

psql -U postgres -d homework -c "\COPY cpu_usage FROM /docker-entrypoint-initdb.d/cpu_usage.csv CSV HEADER"