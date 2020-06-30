#!/bin/bash

for i in $(seq $2)
do
    echo "Run: "$i >> run.txt
    hadoop jar k-means-1.0-SNAPSHOT.jar it.unipi.hadoop.KMeans $1 centroids/centroids$i >> run.txt
    echo "centroids:" >> run.txt
    hadoop fs -get centroids/centroids$i/centroids.txt ./centroids$i.txt
    cat ./centroids$i.txt >> run.txt
    rm ./centroids$i.txt
done