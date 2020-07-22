import sys
import os
import json
from pyspark import SparkContext
from point import Point
import time

def init_centroids(dataset, k):
    start_time = time.time()
    initial_centroids = dataset.takeSample(False, k)
    print("init centroid execution:", len(initial_centroids), "in", (time.time() - start_time), "s")
    return initial_centroids

def assign_centroids(p):
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
    old_centroids = centroids_broadcast.value
    for i in range(len(old_centroids)):
        check = old_centroids[i].distance(new_centroids[i], distance_broadcast.value) <= threshold
        if check == False:
            return False
    return True

if __name__ == "__main__":
    start_time = time.time()
    if len(sys.argv) != 3:
        print("Number of arguments not valid!")
        sys.exit(1)

    with open('./config.json') as config:
        parameters = json.load(config)["configuration"][0]

    INPUT_PATH = str(sys.argv[1])
    OUTPUT_PATH = str(sys.argv[2])
    
    sc = SparkContext("yarn", "Kmeans")
    sc.setLogLevel("ERROR")
    sc.addPyFile("./point.py") ## It's necessary, otherwise the spark framework doesn't see point.py

    print("\n***START****\n")

    points = sc.textFile(INPUT_PATH).map(Point).cache()
    initial_centroids = init_centroids(points, k=parameters["k"])
    distance_broadcast = sc.broadcast(parameters["distance"])
    centroids_broadcast = sc.broadcast(initial_centroids)

    stop, n = False, 0
    while True:
        print("--Iteration n. {itr:d}".format(itr=n+1), end="\r", flush=True)
        cluster_assignment_rdd = points.map(assign_centroids)
        sum_rdd = cluster_assignment_rdd.reduceByKey(lambda x, y: x.sum(y))
        centroids_rdd = sum_rdd.mapValues(lambda x: x.get_average_point()).sortBy(lambda x: x[1].components[0])

        new_centroids = [item[1] for item in centroids_rdd.collect()]
        stop = stopping_criterion(new_centroids,parameters["threshold"])

        n += 1
        if(stop == False and n < parameters["maxiteration"]):
            centroids_broadcast = sc.broadcast(new_centroids)
        else:
            break

    with open(OUTPUT_PATH, "w") as f:
        for centroid in new_centroids:
            f.write(str(centroid) + "\n")

    execution_time = time.time() - start_time
    print("\nexecution time:", execution_time, "s")
    print("average time per iteration:", execution_time/n, "s")
    print("n_iter:", n)
