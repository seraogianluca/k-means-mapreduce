# The K-means Clustering Algorithm in MapReduce

## Table of Contents
1) [Introduction](#1-introduction)
2) [Pseudocode](#2-pseudocode)
3) [Implementation](#3-implementation)
4) [Test](#4-test)

## 1. Introduction
In this document there is a detailed description of our k-means algorithm implementation with the hadoop framework.

## 2. Pseudocode

In a general overview our mapreduce k-means algorithm is:
```
while old_centroids - new_centroids > threshold:
    Map:
        - Given a point and the set of centroids.
        - Calculate the distance between the point and each centroid.
        - Output the point and the closest centroid.
        
    Reduce:
        - Given the centroid i and the list of points belonging to the i-th cluster.
        - Calculate the new centroid as the aritmetic mean position of the given points.
        - Output the new centroid.
```

### 2.1 Mapper
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

The mapper calculates the distance between the point and each centroid. The index of the closest centroid and the point are then emitted.

### 2.2 Combiner
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
Since we need to sum the points belonging to a cluster to calculate the centroid (arithmetic mean of points), and since the sum is an associative and commutative function, our algorithm can benefit from the use of a combiner to reduce the amount of data to be transmitted to the reducers.



### 2.3 Reducer
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
Since the arithmetic mean is not an associative function, we calculate it at the end of the reducer. The result of the mapreduce stage will be the same even if the combiner is not called by the hadoop framework.

## 3. Implementation

### input file format
### output 

## 4. Test
tabellina confronto kmeans python e nostro (benchmark)


# things to be discussed in detail:
treshold as stopping criterion