# The K-means Clustering Algorithm in Hadoop MapReduce

## Table of Contents
1) [Introduction](#1-introduction)
2) [Pseudocode](#2-pseudocode)
3) [Implementation](#3-implementation)
4) [Test](#4-test)

## 1. Introduction
K-Means is a clustering algorithm that partition a set of data point into k clusters. The k-means clustering algorithm is commonly used on large data sets, and because of the characteristics of the algorithm is a good candidate for parallelization. The aim of this project is to implement a framework in java for performing k-means clustering using Hadoop MapReduce. 

## 2. Pseudocode
The classical k-means algorithm works as an iterative  process in which at each iteration it computes the distance between the data points and the centroids, that are randomly initialized at the beginning of the algorithm. 

We decided to design such algorithm as a MapReduce workflow. A single stage of MapReduce roughly corresponds to a single iteration of the classical algorithm. As in the classical algorithm at the first stage the centroids are randomly sampled from the set of data points. The map function takes as input a data point and the list of centroids, computes the distance between the point and each centroid and emits the point and the closest centroid. The reduce function collects all the points beloging to a cluster and computes the new centroid and emits it. At the end of each stage, it finds a new approximation of the centroids, that are used for the next iteration. The workflow continues until the distance from each centroid of a previuos stage and the corresponding centroids of the current stage drops below a given threshold.

```
centroids = k random sampled points from the dataset.

do:

    Map:
        - Given a point and the set of centroids.
        - Calculate the distance between the point and each centroid.
        - Emit the point and the closest centroid.
        
    Reduce:
        - Given the centroid and the points belonging to its cluster.
        - Calculate the new centroid as the aritmetic mean position of the points.
        - Emit the new centroid.      
    
    prev_centroids = centroids.
    centroids = new_centroids.
    
while prev_centroids - centroids > threshold.      
```

### 2.1 Mapper
The mapper calculates the distance between the data point and each centroid. Then emits the index of the closest centroid and the data point.

```
class MAPPER
    method MAP(file_offset, point)
        min_distance = POSITIVE_INFINITY
        closest_centroid = -1
        for all centroid in list_of_centroids
            distance = distance(centroid, point)
            if (distance < min_distance)
                closest_centroid = index_of(centroid)
                min_distance = distance
        EMIT(closest_centroid, point) 
```

### 2.2 Combiner
At each stage we need to sum the data points belonging to a cluster to calculate the centroid (arithmetic mean of points). Since the sum is an associative and commutative function, our algorithm can benefit from the use of a combiner to reduce the amount of data to be transmitted to the reducers.

```
class COMBINER
    method COMBINER(centroid_index, list_of_points)
        point_sum.number_of_points = 0
        point_sum = 0
        for all point in list_of_points:
            point_sum += point
            point_sum.number_of_points += 1
        EMIT(centroid_index, point_sum)    
```

### 2.3 Reducer
The reducer calculates the new approximation of the centroid and emits it. The result of the MapReduce stage will be the same even if the combiner is not called by the Hadoop framework.

```
class REDUCER
    method REDUCER(centroid_index, list_of_point_sums)
        number_of_points = partial_sum.number_of_points
        point_sum = 0
        for all partial_sum in list_of_partial_sums:
            point_sum += partial_sum
            point_sum.number_of_points += partial_sum.number_of_points
        centroid_value = point_sum / point_sum.number_of_points
        EMIT(centroid_index, centroid_value)
```

## 3. Implementation

Hadoop implementation: [K-Means Hadoop](/doc/hadoop-md)
Spark implementation: [K-Means Spark](/doc/spark.md)

## 4. Test

### Dataset creation 
To create artificial datasets to be used for testing the implementation we used the scikit-learn python library. In particular, we used the make_blobs() function that generates isotropic Gaussian blobs for clustering, in order to have a set of points with 

The **make_blobs()** function generates isotropic Gaussian blobs for clustering.
This was made in order to generate datasets with clustering tendency instead of using a random uniformly distributed set of points, because of that, the number of centers parameter choosed is much higher than the actual number of centroids that will be used to test the app.


### Input file 
Example of the [dataset.txt](/k-means/...)

```
-4.2458,-0.6104,8.8017
-5.404,3.2226,3.1959
-5.5864,-2.3265,6.5487
-6.7917,6.2481,3.9821
-7.7237,-6.1956,8.8869
8.32,2.7118,-9.2038
```

### Output file
Example of the final [centroids](/k-means/...) file

```
-4.2458,-0.6104,8.8017
-5.404,3.2226,3.1959
```


------------------------

tabellina confronto kmeans python e nostro (benchmark)

scelta del dataset scriptino pyython


## 5. Credits