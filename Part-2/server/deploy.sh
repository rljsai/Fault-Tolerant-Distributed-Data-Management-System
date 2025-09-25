#!/usr/bin/env bash
set -e

# Start Postgres service
service postgresql start

# Wait for Postgres
sleep 3

# Create DB if not exists
su - postgres -c "psql -tc \"SELECT 1 FROM pg_database WHERE datname='studdb'\" | grep -q 1 || psql -c 'CREATE DATABASE studdb;'"

# ✅ Ensure postgres user has a password (matches env POSTGRES_PASSWORD)
su - postgres -c "psql -c \"ALTER USER postgres WITH PASSWORD 'postgres';\""

# Run Quart app
exec python app.py
