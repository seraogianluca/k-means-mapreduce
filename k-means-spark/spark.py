import sys
import os
import numpy as ny
from pyspark import SparkContext
from point import Point

os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3' ## TODO: Remove
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3' ## TODO: Remove

centroids_broadcast = None
distance_broadcast = None

def init_centroids(input_path, dataset_size, k):
    positions = ny.random.randint(0, high=dataset_size-1, size=k)
    positions.sort()

    initial_centroids = []
    with open(input_path) as fp:
        i = 0
        j = 0
        
        while(i < len(positions)):
            position = positions[i]
            if(j == position):
                line = fp.readline()
                line = line.replace(" ", "").split(",")
                components = [float(i) for i in line]
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
    nearest_centroid = centroids_broadcast.value[0]
    for centroid in centroids_broadcast.value:
        distance = p.distance(centroid, 2)
        if(distance < min_dist):
            min_dist = distance
            nearest_centroid = centroid
    return (nearest_centroid, p)

def combiner(centroid, points):
    res = Point(None)
    for i in points:
        res.sum(points[i])
    return (centroid, sum)   

if __name__ == "__main__":
    sc = SparkContext("local", "Kmeans")
    print(os.getcwd())
    sc.addPyFile("./point.py") ## It's necessary, otherwise the framework doesn't see point
    print("Parameters:", str(len(sys.argv)))
    input_path = str(sys.argv[1])
    output_path = str(sys.argv[2]) if len(sys.argv) > 1 else "./output.txt"
    dataset_size = int(sys.argv[3])
    distance = int(sys.argv[4])
    dim = int(sys.argv[5])
    k = int(sys.argv[6])
    treshold = float(sys.argv[7])

    distance_broadcast = sc.broadcast(k)

    initial_centroids = init_centroids(input_path, dataset_size=dataset_size, k=k)
    centroids_broadcast = sc.broadcast(initial_centroids)
    input_file = sc.textFile(input_path)
    map = input_file.map(lambda row: assign_centroids(row))
    # #centroidsRDD = map.combineByKey(lambda value: (value), lambda x, y: x.sum(y), lambda value: value.get_average_point())    
    # sumRDD = map.reduceByKey(lambda x, y: x.sum(y)) ## f(x) must be associative
    # print("**Reduce correct")
    # centroidsRDD = sumRDD.mapValues(lambda x: x.get_average_point())
    map.saveAsTextFile(output_path)

    