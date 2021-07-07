#!/bin/bash

set -e

for month in $(cat months.txt)
do
    for state_abbrev in $(cat state_abbreviations.txt)
    do
        echo "Running for month $month and state $state_abbrev"
        bash run.sh "$month" "$state_abbrev" replace
        echo "Done"
        echo "------------------------------------------------"
    done
done
