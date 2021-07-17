#!/bin/bash

# Build tap and target configuration JSON files and run extraction
# Usage: 
#    - Load incrementally based on last state within the month:
#        `bash run.sh SC 2021-01-01`
#    - Replace contents and load whole month (useful to run once a month
#      to clean up any duplication issues or get records that were eventually updated):  
#        `bash run.sh SC 2021-01-01 replace`

set -e

extract_until_date=$1
state_abbrev=$2
load_mode=$3

# Prepare year_month value to use as partition identifier
year_month="${extract_until_date::-3}-01"

# Set credentials
aws_key="$UDACITY_AWS_KEY"
aws_secret="$UDACITY_AWS_SECRET"

# Configure S3 path
s3_bucket=$UDACITY_CAPSTONE_PROJECT_BUCKET
base_bucket_path="raw/vaccinations"
s3_prefix="$base_bucket_path/year_month=$year_month/estabelecimento_uf=$state_abbrev"

state_json_filepath="state.json"

# Build configuration files
tap_config_json=$( jq -n \
                  --arg ym "$year_month" \
                  --arg sa "$state_abbrev" \
                  --arg ed "$extract_until_date" \
                  '{disable_collection: true, year_month: $ym, state_abbrev: $sa, extract_until_date: $ed}' )

# S3 CSV
target_config_json=$( jq -n \
                  --arg ak "$aws_key" \
                  --arg ae "$aws_secret" \
                  --arg ap "$AWS_PROFILE" \
                  --arg sb "$s3_bucket" \
                  --arg sp "$s3_prefix/" \
                  --arg qc '"' \
                  --arg dl "," \
                  --arg co "gzip" \
                  '{disable_collection: true, aws_access_key_id: $ak, aws_secret_access_key: $ae, aws_profile: $ap, s3_bucket: $sb, s3_key_prefix: $sp, delimiter: $dl, quotechar: $qc, compression: $co}' )


echo $tap_config_json > config.json
echo $target_config_json > s3_csv_config.json

if [ "$load_mode" = "replace" ]
then
    # Move current files to trash that will be emptied at the end if execution succeeds
    remove_from_destination="s3://$s3_bucket/$s3_prefix"
    trash_destination="$remove_from_destination/trash/"
    echo "Replace mode: moving existing file(s) to $trash_destination"
    aws s3 mv "$remove_from_destination" "$trash_destination" --include "*.csv*" --recursive

    # Set state to null, so that extraction begins on the first day of the month
    null_state_json=$( jq -n \
                  '{"bookmarks": {"vaccinations": {"state_abbrev_from_date": null}}, "currently_syncing": null}' )

    echo $null_state_json > "$state_json_filepath"

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





