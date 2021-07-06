#!/bin/bash

set -e

STATE_ABBREV=$1

for month in $(cat months.txt)
do
    echo "Running for state $STATE_ABBREV and month $month"
    bash run.sh "$STATE_ABBREV" "$month"
    echo "Done"
    echo "------------------------------------------------"
done