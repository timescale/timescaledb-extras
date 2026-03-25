#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

DB="cagg_check_gaps_test"

# Create a fresh database
psql -X -d postgres -c "DROP DATABASE IF EXISTS $DB;" > /dev/null 2>&1
psql -X -d postgres -c "CREATE DATABASE $DB;" > /dev/null 2>&1

# Run setup and queries, capture output
psql -X -d "$DB" -f setup.sql > /dev/null 2>&1
psql -X -d "$DB" -e -f queries.sql &> temp.out 

# Clean up database
psql -X -d postgres -c "DROP DATABASE IF EXISTS $DB;" > /dev/null 2>&1

if diff -u expected.out temp.out > diffs.out 2>&1; then
    rm -f diffs.out
    echo "OK"
else
    echo "FAILED — see diffs.out for details"
    exit 1
fi
