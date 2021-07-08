#!/bin/bash

# Build tap and target configuration JSON files and run extraction
# Usage: 
#    - Extract and load empty destination:     `bash run.sh SC 2021-01-01`
#    - Extract and replace existing contents:  `bash run.sh SC 2021-01-01 replace`

set -e

YEAR_MONTH=$1
STATE_ABBREV=$2
LOAD_MODE=$3

# Set credentials
AWS_KEY="$UDACITY_AWS_KEY"
AWS_SECRET="$UDACITY_AWS_SECRET"
AWS_PROFILE="$UDACITY_AWS_PROFILE"

# Configure S3 path
S3_BUCKET=$UDACITY_CAPSTONE_PROJECT_BUCKET
BASE_BUCKET_PATH="raw/vaccinations"
s3_prefix="$BASE_BUCKET_PATH/year_month=$YEAR_MONTH/estabelecimento_uf=$STATE_ABBREV/"


# Build configuration files
TAP_CONFIG_JSON=$( jq -n \
                  --arg ym "$YEAR_MONTH" \
                  --arg sa "$STATE_ABBREV" \
                  '{disable_collection: true, year_month: $ym, state_abbrev: $sa}' )

# S3 CSV
TARGET_CONFIG_JSON=$( jq -n \
                  --arg ak "$AWS_KEY" \
                  --arg ae "$AWS_SECRET" \
                  --arg ap "$AWS_PROFILE" \
                  --arg sb "$S3_BUCKET" \
                  --arg sp "$s3_prefix" \
                  --arg qc '"' \
                  --arg dl "," \
                  --arg co "gzip" \
                  '{disable_collection: true, aws_access_key_id: $ak, aws_secret_access_key: $ae, aws_profile: $ap, s3_bucket: $sb, s3_key_prefix: $sp, delimiter: $dl, quotechar: $qc, compression: $co}' )


echo $TAP_CONFIG_JSON > config.json
echo $TARGET_CONFIG_JSON > s3_csv_config.json

if [ "$LOAD_MODE" = "replace" ]
then
    # Clean up S3 destination
    remove_from_destination="s3://$S3_BUCKET/$s3_prefix"
    echo "Replace mode: Removing current file at $remove_from_destination"
    aws s3 rm "$remove_from_destination" --profile "$UDACITY_AWS_PROFILE" --include "*.csv*" --recursive
fi

# Run tap and target
make sync-s3-csv
