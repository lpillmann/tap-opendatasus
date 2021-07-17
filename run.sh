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

# Configure S3 path
S3_BUCKET=$UDACITY_CAPSTONE_PROJECT_BUCKET
BASE_BUCKET_PATH="raw/vaccinations"
s3_prefix="$BASE_BUCKET_PATH/year_month=$YEAR_MONTH/estabelecimento_uf=$STATE_ABBREV"


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
                  --arg sp "$s3_prefix/" \
                  --arg qc '"' \
                  --arg dl "," \
                  --arg co "gzip" \
                  '{disable_collection: true, aws_access_key_id: $ak, aws_secret_access_key: $ae, aws_profile: $ap, s3_bucket: $sb, s3_key_prefix: $sp, delimiter: $dl, quotechar: $qc, compression: $co}' )


echo $TAP_CONFIG_JSON > config.json
echo $TARGET_CONFIG_JSON > s3_csv_config.json

if [ "$LOAD_MODE" = "replace" ]
then
    # Move current files to trash that will be emptied at the end if execution succeeds
    remove_from_destination="s3://$S3_BUCKET/$s3_prefix"
    trash_destination="$remove_from_destination/trash/"
    echo "Replace mode: moving existing file(s) to $trash_destination"
    aws s3 mv "$remove_from_destination" "$trash_destination" --include "*.csv*" --recursive

    # Run tap and target
    if make sync-s3-csv
    then
        # Remove trash contents
        echo "Replace mode: emptying trash contents from $trash_destination"
        aws s3 rm "$trash_destination" --recursive
    else
        # Recover from trash
        echo "Replace mode: abort removal due to execution error. Recovering from trash to $remove_from_destination"
        if aws s3 mv "$trash_destination" "$remove_from_destination" --include "*.csv*" --recursive
        then
            # Remove trash folder
            aws s3 rm "$trash_destination" --recursive
        fi
    fi
else
    # Run tap and target just adding new files to the destination
    make sync-s3-csv
fi





