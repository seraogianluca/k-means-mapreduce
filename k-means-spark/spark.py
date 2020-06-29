import sys
import os
import json
import numpy as ny
from pyspark import SparkContext
from point import Point
import time

#os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3' ## TODO: Remove
#os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3' ## TODO: Remove

def init_centroids(dataset, dataset_size, k):
    positions = ny.random.choice(range(dataset_size), size=k, replace=False)
    positions.sort()
    print("Random positions:", positions)
    initial_centroids = []
    i, j = 0, 0
    for row in dataset.collect():
        if (i >= len(positions)):
            break
        position = positions[i]
        if(j == position):
            line = row.replace(" ", "").split(",")
            components = [float(k) for k in line]
            p = Point(components)
            i += 1
            initial_centroids.append(p)
        j += 1
    # print("Last initial centroid:", initial_centroids[len(initial_centroids)-1])
    return initial_centroids

def assign_centroids(row):
    # Create the point
    components = row.replace(" ", "").split(",")
    components = [float(i) for i in components]
    p = Point(components)
    # Assign to the closer centroid
    min_dist = float("inf")
    centroids = centroids_broadcast.value
    nearest_centroid = 0
    for i in range(len(centroids)):
        distance = p.distance(centroids[i], distance_broadcast.value)
        if(distance < min_dist):
            min_dist = distance
            nearest_centroid = i
    return (nearest_centroid, p)

def stopping_criterion(new_centroids, dist, threshold):
    check = True
    old_centroids = centroids_broadcast.value
    for i in range(len(old_centroids)):
        check = old_centroids[i].distance(new_centroids[i], dist) < threshold
        if check == False:
            return False
    return True

def reduce(x, y):
    x.sum(y)
    return x

if __name__ == "__main__":
    start_time = time.time()
    sc = SparkContext("yarn", "Kmeans")
    print("\n***START****\n")
    sc.setLogLevel("ERROR")
    sc.addPyFile("./point.py") ## It's necessary, otherwise the spark framework doesn't see point.py
    config = open('./config.json')
    parameters = json.load(config)["configuration"][0]
    config.close()    
    print("Parameters:", str(len(sys.argv)))
    if len(sys.argv) < 2:
        print("Number of arguments not valid!")
        sys.exit(1)
    INPUT_PATH = str(sys.argv[1])
    OUTPUT_PATH = str(sys.argv[2]) if len(sys.argv) > 1 else "./output.txt"
    DISTANCE_TYPE = parameters["distance"]
    THRESHOLD = parameters["threshold"]
    MAX_ITERATIONS = parameters["maxiteration"]

    input_file = sc.textFile(INPUT_PATH)
    initial_centroids = init_centroids(input_file, dataset_size=parameters["datasetsize"], k=parameters["k"])
    print("Centroids initialized:", len(initial_centroids), "in", (time.time() - start_time))
    distance_broadcast = sc.broadcast(DISTANCE_TYPE)
    centroids_broadcast = sc.broadcast(initial_centroids)
    stop, n = False, 0
    while stop == False and n < MAX_ITERATIONS:
        print("**Iteration n." + str(n+1))
        result = []
        map = input_file.map(lambda row: assign_centroids(row))
        sumRDD = map.reduceByKey(lambda x, y: reduce(x,y)) ## f(x) must be associative
        centroidsRDD = sumRDD.mapValues(lambda x: x.get_average_point())
        new_centroids = [item[1] for item in centroidsRDD.collect()]
        stop = stopping_criterion(new_centroids, DISTANCE_TYPE,THRESHOLD)
        n += 1
        if(stop == False and n < MAX_ITERATIONS):
            centroids_broadcast.unpersist()
            centroids_broadcast = sc.broadcast(new_centroids)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    already_exists = fs.exists(sc._jvm.org.apache.hadoop.fs.Path(OUTPUT_PATH + "/_SUCCESS"))
    if already_exists:
        fs.delete(sc._jvm.org.apache.hadoop.fs.Path(OUTPUT_PATH), True)
        print("File already exists, it has been overwritten")
    centroidsRDD.saveAsTextFile(OUTPUT_PATH)
    print("\nIterations:", n)
    print("Time:", (time.time() - start_time), "s")