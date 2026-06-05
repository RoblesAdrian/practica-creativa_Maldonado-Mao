#!/bin/bash
set -e

echo "Importing origin_dest_distances into MongoDB..."
mongoimport \
  --db "${MONGO_INITDB_DATABASE:-agile_data_science}" \
  --collection origin_dest_distances \
  --file /seed-data/origin_dest_distances.jsonl \
  --drop
