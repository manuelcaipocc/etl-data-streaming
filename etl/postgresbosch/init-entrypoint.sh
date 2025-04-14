#!/bin/bash
set -e

echo "Esperando que PostgreSQL esté disponible..."
until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"; do
  sleep 2
done

echo "Ejecutando script de cron jobs..."
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/register_cron_jobs.sql

echo "Cron jobs registrados correctamente."
