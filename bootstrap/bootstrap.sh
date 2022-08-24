#!/bin/sh

set -e

export PGHOST=${PGHOST:-localhost}
export PGPORT=${PGPORT:-5432}
export PGUSER=${PGUSER:-qu}
export PGPASSWORD=${PGPASSWORD:-qu}

cat ./qu.sql | psql
