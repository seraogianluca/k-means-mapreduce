#!/bin/bash

# $1: path dataset 
# $2: number of run
# $3: thousands of points
# $4: dimension
# $5: k

for i in $(seq $2)
do
    echo "Run: "$i >> run.txt
    spark-submit --master yarn spark.py $1 "out/spark/$3k/$4_$5/centroids_$i" >> run.txt
    echo "centroids:" >> run.txt
    hadoop fs -get "out/spark/$3k/$4_$5/centroids_$i/part-00000" ./centroids$i.txt
    cat ./centroids$i.txt >> run.txt
    rm ./centroids$i.txt
done