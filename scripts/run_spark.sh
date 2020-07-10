#!/bin/bash

# $1: path dataset 
# $2: number of run
# $3: thousands of points
# $4: dimension
# $5: k

for i in $(seq $2)
do
    echo "Run: "$i >> run.txt
    spark-submit --master yarn spark.py $1 "output.txt" >> run.txt
    echo "centroids:" >> run.txt
    cat "output.txt" >> run.txt
    echo "" >> run.txt
done