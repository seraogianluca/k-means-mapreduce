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
    # positions = ny.random.randint(0, high=dataset_size-1, size=k)
    positions = ny.random.choice(range(dataset_size), size=k, replace=False)
    positions.sort()
    print(positions)

    initial_centroids = []
    with open(input_path) as fp:
        i = 0
        j = 0
        
        while(i < len(positions)):
            position = positions[i]
            line = fp.readline()
            if(j == position):
                print(j)
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
        distance = p.distance(centroid, distance_broadcast.value)
        if(distance < min_dist):
            min_dist = distance
            nearest_centroid = centroid
    return (nearest_centroid, p)

def stopping_criterion(old, new, dist, threshold):
    check = True
    for i in range(0, len(old)):
        check = old[i].distance(new[i], dist) < threshold
        if check == False:
            return False
    return True

def reduce(x, y):
    x.sum(y)
    return x

if __name__ == "__main__":
    sc = SparkContext("local", "Kmeans")
    print(os.getcwd())
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

    distance_broadcast = sc.broadcast(distance)

    initial_centroids = init_centroids(input_path, dataset_size=dataset_size, k=k)
    print("Initial centroids nr.:", len(initial_centroids))
    centroids_broadcast = sc.broadcast(initial_centroids)
    input_file = sc.textFile(input_path)
    stop = False
    n = 0
    while stop == False and n < max_iterations:
        map = input_file.map(lambda row: assign_centroids(row))
        sumRDD = map.reduceByKey(lambda x, y: reduce(x,y)) ## f(x) must be associative
        centroidsRDD = sumRDD.mapValues(lambda x: x.get_average_point())
        new_centroids = []
        for tpl in centroidsRDD.collect():
            new_centroids.append(tpl[1])

        print(centroids_broadcast.value)
        stop = stopping_criterion(centroids_broadcast.value, new_centroids, distance,treshold)
        n += 1
        if(stop == False and n < max_iterations):
            centroids_broadcast = sc.broadcast(new_centroids)
 
    print("Iterations:", n)    
    #centroidsRDD.saveAsTextFile(output_path)
    for tpl in centroidsRDD.collect():
        print(tpl[1])

    



 

