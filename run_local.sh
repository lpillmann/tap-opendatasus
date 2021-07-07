#!/bin/bash

# Build tap and target configuration JSON files and run extraction saving CSVs locally
# Usage: `bash run.sh SC 2021-01-01`

set -e

STATE_ABBREV=$1
YEAR_MONTH=$2

# For local extraction
BASE_DATA_PATH="./data"
destination_path="$BASE_DATA_PATH/$STATE_ABBREV/$YEAR_MONTH"
Create destination directory
mkdir -p "$destination_path"

# Build configuration files
TAP_CONFIG_JSON=$( jq -n \
                  --arg ym "$YEAR_MONTH" \
                  --arg sa "$STATE_ABBREV" \
                  '{disable_collection: true, year_month: $ym, state_abbrev: $sa}' )

# Local CSV
TARGET_CONFIG_JSON=$( jq -n \
                  --arg dp "$destination_path" \
                  --arg qc "'" \
                  --arg dl "," \
                  '{disable_collection: true, delimiter: $dl, quotechar: $qc, destination_path: $dp}' )


echo $TAP_CONFIG_JSON > config.json
echo $TARGET_CONFIG_JSON > csv_config.json

# Run tap and target
make sync-csv
