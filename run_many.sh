set -e

for month in $(cat months.txt)
do
    echo $month
    bash run.sh "MA" $month
done