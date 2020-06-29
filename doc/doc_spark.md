# The K-means Clustering Algorithm in Spark

## Table of Contents
1) [Implementation](#1-implementation)
2) [Test](#2-test)

## 1. Implementation

### 1.1 Model

In order to represent the points, a class **Point** has been defined.
It's characterized by the following fields:

- array of components
- dimension
- number of points: a point can be seen as the aggregation of many points, so this variable is used to track the number of points that are represented by the object

It includes the following operations:

- distance (is possible to pass as parameter the type of distance)
- sum
- get_average

```
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

### 1.1 Mapper

The mapper method is invoked, at each iteration,  on the input file, that contains the points from the dataset.


```
 map = input_file.map(lambda row: assign_centroids(row))
```
(*line 90 of [spark.py](/k-means-spark/spark.py)*)

The **assign_centroid** function is divided in two parts: in the first we read a point (as a row of coordinates);in the second we assign the point to the closest centroid, using the type of distance specified by the user, ans we emit the result as a tuple: **(id of the centroid, point)**.
```

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

## 2. Test

