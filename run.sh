# Build tap and target configuration JSON files and run extraction
# Usage: `bash run.sh SC 2021-01-01`

set -e

STATE_ABBREV=$1
YEAR_MONTH=$2

# For local extraction
BASE_DATA_PATH="./data"
destination_path="$BASE_DATA_PATH/$STATE_ABBREV/$YEAR_MONTH"

# For S3 extraction
BASE_BUCKET_PATH="raw/vaccines"
s3_prefix="$BASE_BUCKET_PATH/$STATE_ABBREV/$YEAR_MONTH/"

# Replace with your credentials (define them in your bash profile - ~/.zshrc in my case)
S3_BUCKET=$UDACITY_CAPSTONE_PROJECT_BUCKET
AWS_KEY="$UDACITY_AWS_KEY"
AWS_SECRET="$UDACITY_AWS_SECRET"
AWS_PROFILE="$UDACITY_AWS_PROFILE"

# Build configuration files
TAP_CONFIG_JSON=$( jq -n \
                  --arg ym "$YEAR_MONTH" \
                  --arg sa "$STATE_ABBREV" \
                  '{disable_collection: true, year_month: $ym, state_abbrev: $sa}' )

# Local CSV
# TARGET_CONFIG_JSON=$( jq -n \
#                   --arg dp "$destination_path" \
#                   --arg qc "'" \
#                   --arg dl "," \
#                   '{disable_collection: true, delimiter: $dl, quotechar: $qc, destination_path: $dp}' )

# S3 CSV
TARGET_CONFIG_JSON=$( jq -n \
                  --arg ak "$AWS_KEY" \
                  --arg ae "$AWS_SECRET" \
                  --arg ap "$AWS_PROFILE" \
                  --arg sb "$S3_BUCKET" \
                  --arg sp "$s3_prefix" \
                  --arg co "gzip" \
                  --arg qc "'" \
                  --arg dl "," \
                  '{disable_collection: true, aws_access_key_id: $ak, aws_secret_access_key: $ae, aws_profile: $ap, s3_bucket: $sb, s3_key_prefix: $sp, delimiter: $dl, quotechar: $qc, compression: $co}' )


echo $TAP_CONFIG_JSON > config.json
echo $TARGET_CONFIG_JSON > s3_csv_config.json

# Create destination directory
mkdir -p "$destination_path"

# Run tap and target
make sync-s3-csv
