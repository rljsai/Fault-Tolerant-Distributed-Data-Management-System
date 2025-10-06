#!/usr/bin/env bash
set -e

# DB_NAME=${DB_NAME:-studdb}
# DB_USER=${POSTGRES_USER:-postgres}
# DB_PASSWORD=${POSTGRES_PASSWORD:-postgres}
# Start Postgres service in background
service postgresql start

# Wait for Postgres to be ready
sleep 3

# Create the database if it doesn't exist
#su - postgres -c "psql -tc \"SELECT 1 FROM pg_database WHERE datname='$DB_NAME'\" | grep -q 1 || psql -c 'CREATE DATABASE $DB_NAME;'"

# Ensure postgres user has password
#su - postgres -c "psql -c \"ALTER USER postgres WITH PASSWORD '$DB_PASSWORD';\""

# Run the Quart app
exec python app.py
