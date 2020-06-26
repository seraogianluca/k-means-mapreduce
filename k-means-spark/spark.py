import sys
import os
import numpy as ny
from pyspark import SparkContext
from point import Point

# os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3' ## TODO: Remove
# os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3' ## TODO: Remove

# centroids_broadcast
# distance_broadcast

def init_centroids(dataset, dataset_size, k):
    positions = ny.random.choice(range(dataset_size), size=k, replace=False)
    positions.sort()
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
    return nearest_centroid, p

def stopping_criterion(old, dist, threshold):
    check = True
    old_centroids = centroids_broadcast.value
    for i in range(len(old_centroids)):
        check = old_centroids[i].distance(result[i][1], dist) < threshold
        if check == False:
            return False
    return True

def reduce(x, y):
    x.sum(y)
    return x

if __name__ == "__main__":
    sc = SparkContext("yarn", "Kmeans")
    print("\n***START****\n")
    sc.setLogLevel("ERROR")
    sc.addPyFile("./point.py") ## It's necessary, otherwise the spark framework doesn't see point.py
    print("Parameters:", str(len(sys.argv)))
    if len(sys.argv) < 9:
        print("Number of arguments not valid!")
        sys.exit(1)
    input_path = str(sys.argv[1])
    output_path = str(sys.argv[2]) if len(sys.argv) > 1 else "./output.txt"
    dataset_size = int(sys.argv[3])
    distance = int(sys.argv[4])
    dim = int(sys.argv[5])
    k = int(sys.argv[6])
    treshold = float(sys.argv[7])
    max_iterations = float(sys.argv[8])

    input_file = sc.textFile(input_path)
    initial_centroids = init_centroids(input_file, dataset_size=dataset_size, k=k)
    distance_broadcast = sc.broadcast(distance)
    centroids_broadcast = sc.broadcast(initial_centroids)
    print("Iteration", 0)
    print("Centroid broadcast:", centroids_broadcast.value)
    stop = False
    n = 0
    while stop == False and n < max_iterations:
        print("Iteration", n+1)
        result = []
        map = input_file.map(lambda row: assign_centroids(row))
        sumRDD = map.reduceByKey(lambda x, y: reduce(x,y)) ## f(x) must be associative
        centroidsRDD = sumRDD.mapValues(lambda x: x.get_average_point())
        result = centroidsRDD.collect()
        stop = stopping_criterion(result, distance,treshold)
        n += 1
        if(stop == False and n < max_iterations):
            centroids_broadcast.unpersist() 
            centroids_broadcast.destroy()
            centroids_broadcast = sc.broadcast([tpl[1] for tpl in result])
    print("Iterations:", n)
    print("\n***Centroids***\n")
    #centroidsRDD.saveAsTextFile(output_path)
    for tpl in centroidsRDD.collect():
        print(tpl[1])