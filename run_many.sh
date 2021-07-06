set -e

STATE_ABBREV=$1

for month in $(cat months.txt)
do
    echo $month
    bash run.sh "$STATE_ABBREV" $month
done