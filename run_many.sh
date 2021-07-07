#!/bin/bash

set -e

STATE_ABBREV=$1

for month in $(cat months.txt)
do
    echo "Running for month $month and state $STATE_ABBREV"
    bash run.sh "$month" "$STATE_ABBREV" replace
    echo "Done"
    echo "------------------------------------------------"
done