# K-Means - Spark implementation

## Table of Contents

1) [Initializzation and stages](#1-initialization-and-stages)
2) [Model](#2-model)
3) [Mapper](#3-mapper)
4) [Reducer](#4-reducer)
5) [Broadcast](#4-broadcast)

## 1. Initialization and stages

As first step, we read the file with the points and we generate the initial centroids with a random sampling, using the **takeSample()** function. We store the data in the RDD with the **cache()** method.

```python
 points = sc.textFile(INPUT_PATH).map(Point).cache()
 initial_centroids = init_centroids(points, k=parameters["k"])
```

(*line 55-56 of [spark.py](/k-means-spark/spark.py)*)

```python
def init_centroids(dataset, k):
    start_time = time.time()
    initial_centroids = dataset.takeSample(False, k)
    print("init centroid execution:", len(initial_centroids), "in", (time.time() - start_time), "s")
    return initial_centroids
```

(*line 12-16 of [spark.py](/k-means-spark/spark.py)*)

After that, we iterate the mapper and the reducer stages until the **stopping criterion** is verified or when the **maximum number of iterations** is reached.

```python
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
```
(*line 61-74 of [spark.py](/k-means-spark/spark.py)*)

In particular, the stopping condition is computed in this way:

```python
def stopping_criterion(new_centroids, threshold):
    old_centroids = centroids_broadcast.value
    for i in range(len(old_centroids)):
        check = old_centroids[i].distance(new_centroids[i], distance_broadcast.value) <= threshold
        if check == False:
            return False
    return True
```
(*line 29-35 of [spark.py](/k-means-spark/spark.py)*)

## 2. Model

In order to represent the points, a class **Point** has been defined.
It's characterized by the following fields:

- a numpyarray of components
- number of points: a point can be seen as the aggregation of many points, so this variable is used to track the number of points that are represented by the object

It includes the following operations:

- distance (it is possible to pass as parameter the type of distance)
- sum
- get_average_point: this method returns a point that has as components the average of the actual components on the number of the points represented by the object

```python
class Point:
    
    def __init__(self, line):
        values = line.split(",")
        self.components = np.array([round(float(k), 5) for k in values])
        self.number_of_points = 1

    def sum(self, p):
        self.components = np.add(self.components, p.components)
        self.number_of_points += p.number_of_points
        return self

    def distance(self, p, h):
        if (h < 0):
           h = 2
        return linalg.norm(self.components - p.components, h)

    def get_average_point(self):
        self.components = np.around(np.divide(self.components, self.number_of_points), 5)
        return self
```

(*lines 4-23 of [point.py](/k-means-spark/point.py)*)

## 3. Mapper

The mapper method is invoked, at each iteration,  on the input file, that contains the points from the dataset.

```python
 cluster_assignment_rdd = points.map(assign_centroids)
```

(*line 63 of [spark.py](/k-means-spark/spark.py)*)

The **assign_centroids** function, for each point on which is invoked, assign the closest centroid to that point. The centroids are taken from the broadcast variable. The function returns the result as a tuple **(id of the centroid, point)**.


```python

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
```

(*lines 18-27 of [spark.py](/k-means-spark/spark.py)*)

## 4. Reducer

The reduce stage is done using two spark transformations:

- reduceByKey: for each cluster, compute the sum of the points belonging to it. It is mandatory to pass one associative function as a parameter. The associative function (which accepts two arguments and returns a single element) should be commutative and associative in mathematical nature.

```python
sum_rdd = cluster_assignment_rdd.reduceByKey(lambda x, y: x.sum(y))
```

(*line 64 of [spark.py](/k-means-spark/spark.py)*)


- mapValues: it is used to calculate the average point for each cluster at the end of each stage. The points are already divided by key. This trasformation works only on the value of a key. The results are sorted in order to make easier comparisons.

```python
centroids_rdd = sum_rdd.mapValues(lambda x: x.get_average_point()).sortBy(lambda x: x[1].components[0])
```

(*line 65 of [spark.py](/k-means-spark/spark.py)*)

The **get_average_point()** function returns the new computed centroid.
```python
 def get_average_point(self):
        self.components = np.around(np.divide(self.components, self.number_of_points), 5)
        return self
```

(*line 21-23 of [point.py](/k-means-spark/point.py)*)

## 5. Broadcast

We pass the new centroids to the next stage with a read-only global variable provided by the framework. This variable is defined into the Driver and broadcasted to the workers.
The Driver initializes the variable.

```python
centroids_broadcast = sc.broadcast(initial_centroids)
```

(*line 58 of [spark.py](/k-means-spark/spark.py)*)

The workers read the value and perform the operations.

```python
centroids = centroids_broadcast.value
```

(*line 20 of [spark.py](/k-means-spark/spark.py)*)

At the end of each iteration the new centroids are broadcasted.

```python
  if(stop == False and n < parameters["maxiteration"]):
            centroids_broadcast = sc.broadcast(new_centroids)
```

(*line 71-72 of [spark.py](/k-means-spark/spark.py)*)