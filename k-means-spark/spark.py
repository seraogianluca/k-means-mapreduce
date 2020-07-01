import sys
import os
import json
import numpy as ny
from pyspark import SparkContext
from point import Point
import time

# os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3' ## TODO: Remove
# os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3' ## TODO: Remove

def init_centroids(dataset, dataset_size, k):
    start_time = time.time()
    positions = ny.random.choice(range(dataset_size), size=k, replace=False)
    positions.sort()
    initial_centroids = []
    i, j = 0, 0
    for row in dataset.collect():
        if(j == positions[i]):
            line = row.replace(" ", "").split(",")
            initial_centroids.append(Point(line))
            i += 1
            if (i >= len(positions)): 
                break
        j += 1
    print("Centroids initialization:", len(initial_centroids), "in", (time.time() - start_time), "s")
    return initial_centroids

def assign_centroids(row):
    # Create the point
    line = row.replace(" ", "").split(",")
    p = Point(line)
    # Assign to the closest centroid
    min_dist = float("inf")
    centroids = centroids_broadcast.value
    nearest_centroid = 0
    for i in range(len(centroids)):
        distance = p.distance(centroids[i], distance_broadcast.value)
        if(distance < min_dist):
            min_dist = distance
            nearest_centroid = i
    return (nearest_centroid, p)

def stopping_criterion(new_centroids, threshold):
    # O(dim *  k)
    old_centroids = centroids_broadcast.value
    for i in range(len(old_centroids)):
        check = old_centroids[i].distance(new_centroids[i], distance_broadcast.value) < threshold
        if check == False:
            return False
    return True

def clean_hdfs(context, output_path):
    fs = context._jvm.org.apache.hadoop.fs.FileSystem.get(context._jsc.hadoopConfiguration())
    already_exists = fs.exists(sc._jvm.org.apache.hadoop.fs.Path(OUTPUT_PATH + "/_SUCCESS"))
    if already_exists:
        fs.delete(sc._jvm.org.apache.hadoop.fs.Path(OUTPUT_PATH), True)
        print("File already exists, it has been overwritten")

if __name__ == "__main__":
    start_time = time.time()
    if len(sys.argv) < 2:
        print("Number of arguments not valid!")
        sys.exit(1)
    with open('./config.json') as config:
        parameters = json.load(config)["configuration"][0]
    INPUT_PATH = str(sys.argv[1])
    OUTPUT_PATH = str(sys.argv[2]) if len(sys.argv) > 1 else "./output.txt"
    sc = SparkContext("local", "Kmeans")
    print("\n***START****\n")
    sc.setLogLevel("ERROR")
    sc.addPyFile("./point.py") ## It's necessary, otherwise the spark framework doesn't see point.py
    input_file = sc.textFile(INPUT_PATH)
    initial_centroids = init_centroids(input_file, dataset_size=parameters["datasetsize"], k=parameters["k"])
    distance_broadcast = sc.broadcast(parameters["distance"])
    centroids_broadcast = sc.broadcast(initial_centroids)
    stop, n = False, 0
    stages_time = time.time()
    while True:
        print("--Iteration n. {itr:d}".format(itr=n+1), end="\r", flush=True)
        cluster_assignment_rdd = input_file.map(lambda row: assign_centroids(row))
        sum_rdd = cluster_assignment_rdd.reduceByKey(lambda x, y: x.sum(y)) ## f(x) must be associative
        centroids_rdd = sum_rdd.mapValues(lambda x: x.get_average_point()).sortBy(lambda x: x[1].components[0])
        new_centroids = [item[1] for item in centroids_rdd.collect()]
        stop = stopping_criterion(new_centroids,parameters["threshold"])
        n += 1
        if(stop == False and n < parameters["maxiteration"]):
            centroids_broadcast.unpersist()
            centroids_broadcast = sc.broadcast(new_centroids)
        else:
            break
    stages_time = time.time() - stages_time
    clean_hdfs(sc, OUTPUT_PATH)
    centroids_rdd.repartition(1).saveAsTextFile(OUTPUT_PATH)
    print("\nIterations:", n)
    print("Stages time:", stages_time, "s\n")
    print("Average stage time:", stages_time/n, "s\n")
    print("Total time:", (time.time() - start_time), "s")
    print("Overhead time:", (time.time() - start_time) - stages_time, "s\n")
