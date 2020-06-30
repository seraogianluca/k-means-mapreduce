# The K-means Clustering Algorithm in Spark

## Table of Contents

1) [Initializzation and stages](#1-initialization-and-stages)
2) [Model](#2-model)
3) [Mapper](#3-mapper)
4) [Reducer](#4-reducer)
5) [Broadcast](#4-broadcast)

## 1. Initialization and stages

As first step, we read the file with the points and we generate the initial centroids with a random sampling.

```python
initial_centroids = init_centroids(input_file, dataset_size=parameters["datasetsize"], k=parameters["k"])
```

(*line 81 of [spark.py](/k-means-spark/spark.py)*)

```python
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
```

(*line 12-30 of [spark.py](/k-means-spark/spark.py)*)

After that, we iterate the mapper and the reducer stages until the **stopping criterion** is verified or when the **maximum number of iterations** is reached.

```python
 while stop == False and n < MAX_ITERATIONS:
        map = input_file.map(lambda row: assign_centroids(row))
        sumRDD = map.reduceByKey(lambda x, y: reduce(x,y))
        centroidsRDD = sumRDD.mapValues(lambda x: x.get_average_point())
        new_centroids = [item[1] for item in centroidsRDD.collect()]
        stop = stopping_criterion(new_centroids, DISTANCE_TYPE,THRESHOLD)
        n += 1
        if(stop == False and n < MAX_ITERATIONS):
            centroids_broadcast.unpersist()
            centroids_broadcast = sc.broadcast(new_centroids)
```

(*line 86-96 of [spark.py](/k-means-spark/spark.py)*)

## 2. Model

In order to represent the points, a class **Point** has been defined.
It's characterized by the following fields:

- array of components
- dimension
- number of points: a point can be seen as the aggregation of many points, so this variable is used to track the number of points that are represented by the object

It includes the following operations:

- distance (it is possible to pass as parameter the type of distance)
- sum
- get_average: this method returns a point that has as component the average of the actual components on the number of the points represented by the object

```python
class Point:
    def __init__(self, a):
        self.components = a
        self.dimension = len(a)
        self.number_of_points = 1

    def sum(self, p):
        for i in range(0, self.dimension):
            self.components[i] = self.components[i] + p.components[i]
        self.number_of_points += p.number_of_points

    def distance(self, p, h):
        if (h == 0):
           return -1  
        if (h == float("inf")):
            # Chebyshev distance
            max = -1.0
            diff = 0.0
            for i in range(0, self.dimension):
                diff = abs(self.components[i] - p.components[i])
                if (diff > max):
                    max = diff
            return max
        else:
            dist = 0.0
            for i in range(0, self.dimension):
                dist += abs(self.components[i] - p.components[i]) ** h
            dist = dist ** (1.0/h)
            return dist

    def get_average_point(self):
        average_point = Point(self.components)
        for i in range(0, self.dimension):
            average_point.components[i] /= self.number_of_points
        return average_point
```

(*lines 0-38 of [point.py](/k-means-spark/point.py)*)

## 3. Mapper

The mapper method is invoked, at each iteration,  on the input file, that contains the points from the dataset.

```python
 map = input_file.map(lambda row: assign_centroids(row))
```

(*line 89 of [spark.py](/k-means-spark/spark.py)*)

The **assign_centroids** function is divided in two parts: in the first we read a point (as a row of coordinates);in the second we assign the point to the closest centroid, using the type of distance specified by the user, and we emit the result as a tuple: **(id of the centroid, point)**.

```python

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
```

(*lines 32-46 of [spark.py](/k-means-spark/spark.py)*)

## 4. Reducer

The reduce stage is done using two spark transformations:

- reduceByKey: for each cluster, compute the sum of the points belonging to it. It is mandatory to pass one associative function as a parameter.The associative function (which accepts two arguments and returns a single element) should be Commutative and Associative in mathematical nature.

```python
sumRDD = map.reduceByKey(lambda x, y: reduce(x,y))
```

(*line 90 of [spark.py](/k-means-spark/spark.py)*)

```python
def reduce(x, y):
    x.sum(y)
    return x
```

(*line 57-59 of [spark.py](/k-means-spark/spark.py)*)

- mapValues: it is used to calculate the average point for each cluster at the end of each stage. The points are already divided by key. This trasformation works only on the value of a key.

```python
centroidsRDD = sumRDD.mapValues(lambda x: x.get_average_point())
```

(*line 91 of [spark.py](/k-means-spark/spark.py)*)

```python
def get_average_point(self):
        average_point = Point(self.components)
        for i in range(0, self.dimension):
            average_point.components[i] /= self.number_of_points
        return average_point
```

(*line 34-38 of [point.py](/k-means-spark/point.py)*)

## 5. Broadcast

We pass the new centroids to the next stage with a read-only global variable provided by the framework. This variable is defined into the Driver and broadcasted to the workers.
The Driver initializes the variable.

```python
centroids_broadcast = sc.broadcast(initial_centroids)
```

(*line 84 of [spark.py](/k-means-spark/spark.py)*)

The workers read the value and perform the operations.

```python
centroids = centroids_broadcast.value
```

(*line 39 of [spark.py](/k-means-spark/spark.py)*)

At the end of each iteration the variable with old centroids is unpersisted and the new centroids are broadcasted.

```python
 centroids_broadcast.unpersist()
 centroids_broadcast = sc.broadcast(new_centroids)
```

(*line 95-96 of [spark.py](/k-means-spark/spark.py)*)