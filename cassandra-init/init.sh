#!/bin/bash
set -e

echo "Waiting for Cassandra..."
until cqlsh "${CASSANDRA_HOST}" "${CASSANDRA_PORT}" -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; do
  sleep 2
done

echo "Creating schema..."
cqlsh "${CASSANDRA_HOST}" "${CASSANDRA_PORT}" -f /scripts/01_schema.cql

echo "Loading full dataset..."
cqlsh "${CASSANDRA_HOST}" "${CASSANDRA_PORT}" -e "COPY agile_data_science.origin_dest_distances (origin, dest, distance) FROM '/seed-data/origin_dest_distances.csv' WITH HEADER = TRUE;"

echo "Creating prediction results table..."
cqlsh "${CASSANDRA_HOST}" "${CASSANDRA_PORT}" -e "
CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_ml_results (
  uuid text PRIMARY KEY,
  origin text,
  dest text,
  carrier text,
  flightnum text,
  depdelay double,
  distance double,
  dayofweek int,
  dayofmonth int,
  dayofyear int,
  timestamp timestamp,
  prediction int
);"

echo "Cassandra initialized successfully."
