# The K-means Clustering Algorithm

## Table of Contents
1) [Introduction](#1-introduction)
2) [Pseudocode](#2-pseudocode)
3) [Implementation](#3-implementation)
4) [Validation](#4-validation)
5) [Test](#5-test)
6) [Credits](#6-credits)

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
We implemented the combiner only in the Hadoop algorithm.
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

Hadoop implementation: [K-Means Hadoop](/doc/hadoop.md)

Spark implementation: [K-Means Spark](/doc/spark.md)


## 4.Validation

### 2D-dataset
To generate the datasets we used the scikit-learn python library.

We built the dataset using the make_blobs() function of the datasets module to generate points with clustering tendency.

The validation dataset has 1000 2-dimensional points distributed in 4 well defined clusters.


Dataset extract:

```
-0.6779,  10.0174
-0.5575,   7.8922
-7.2418,  -2.1716
 5.3906,  -0.4539
 8.026,    0.4876
-1.277,   -0.344
 6.7044,  -0.5083
...
```
![4clusters.png](/doc/img/4clusters.png)

### Results 
To validate our implementations we used the sklearn KMeans() function of the cluster module. In the following table our MapReduce and Spark executions are compared with the benchmark one.   

| | sklearn.cluster.KMeans | MapReduce | Spark |
| :---- | :----: | :----: | :----: |
|Execution time | 25.9312 ms| 144910 ms| 23716 ms|
|Number of iterations | 2 |6|6|

The tre different implementations returned the same centroids: 

```
-6.790731, -1.783768
-0.652342,  0.645576
-0.18393,   9.132927 
 6.606926,  0.39976 
```

![centroids.png](/doc/img/centroids.png)



## 5. Test
We use datasets with 1000, 10000, 100000 points. For each one of them we have a dataset with:
- 3-dimensional points with 7 centroids.
- 3-dimensional points with 13 centroids.
- 7-dimensional points with 7 centroids.
- 7-dimensional points with 13 centroids.

For each dataset we execute the algorithms **10 times**.

Considered that the k-means algorithm is sensitive to the initial centroids and that we used a random initialization, we will show the **iteration average execution time**. Moreover, we will show the time needed for the centroids initialization.

### 5.1 Datasets with dimension = 3 and k = 7

**Hadoop:**

*Iteration times:*

| Number of samples |Average | Confidence | Variance |
| :---- | :----: | :----: | :----: |
|1000|25.7797 s|±0.4326|0.3290|
|10000|26.1982 s|±0.4064|0.2904|
|100000|26.6614 s|±0.3595|0.2273|

*Centroids initialization:*

| Number of samples | Average | Confidence | Variance |
| :---- | :----: | :----: | :----: |
|1000|1.4238 s|±0.0385|0.0026|
|10000|1.4534 s|±0.0757|0.0101|
|100000|1.5838 s|±0.1532|0.0413|

**Spark:**

*Iteration times:*

| Number of samples |Average | Confidence | Variance |
| :---- | :----: | :----: | :----: |
|1000|5.0593 s|±1.9307|6.5924|
|10000|2.6685 s|±0.5109|0.4590|
|100000|8.2463 s|±1.6511|4.7950|

*Centroids initialization:*

| Number of samples | Average | Confidence | Variance |
| :---- | :----: | :----: | :----: |
|1000|3.8501 s|±0.9756|1.6739|
|10000|3.5767 s|±0.5032|0.4454|
|100000|5.0867 s|±0.922|1.4948|

![comparison](/doc/img/3_7.png)

### 5.2 Datasets with dimension = 3 and k = 13

**Hadoop:**

*Iteration times:*

| Number of samples |Average | Confidence | Variance |
| :---- | :----: | :----: | :----: |
|1000|31.9421 s|±0.7764|1.0602|
|10000|29.5620 s|±1.7928|5.6524|
|100000|30.9473 s|±1.0163|1.8167|

*Centroids initialization:*

| Number of samples | Average | Confidence | Variance |
| :---- | :----: | :----: | :----: |
|1000|1.3553 s|±0.0545|0.0052|
|10000|1.3376 s|±0.3198|0.1798|
|100000|1.5994 s|±0.2061|0.0747|

**Spark:**

*Iteration times:*

| Number of samples |Average | Confidence | Variance |
| :---- | :----: | :----: | :----: |
|1000|3.5078 s|±0.7983|1.1207|
|10000|3.0669 s|±0.6174|0.6704|
|100000|9.9684 s|±1.8765|6.1925|

*Centroids initialization:*

| Number of samples | Average | Confidence | Variance |
| :---- | :----: | :----: | :----: |
|1000|4.2371 s|±1.1114|2.1725|
|10000|4.0950 s|±0.9565|1.6088|
|100000|5.0730 s|±1.33|3.1110|

![comparison](/doc/img/3_13.png)

### 5.3 Datasets with dimension = 7 and k = 7

**Hadoop:**

*Iteration times:*

| Number of samples |Average | Confidence | Variance |
| :---- | :----: | :----: | :----: |
|1000|28.0791 s|±0.9341|6.0496|
|10000|26.2471 s|±0.5073|0.4526|
|100000|26.4312 s|±0.853|1.2795|

*Centroids initialization:*

| Number of samples | Average | Confidence | Variance |
| :---- | :----: | :----: | :----: |
|1000|1.5372 s|±0.0997|0.0689|
|10000|1.6277 s|±0.2277|0.0912|
|100000|1.5396 s|±0.1354|0.0323|

**Spark:**

*Iteration times:*

| Number of samples |Average | Confidence | Variance |
| :---- | :----: | :----: | :----: |
|1000|3.9319 s|±1.2778|2.8716|
|10000|3.2115 s|±1.2891|2.9225|
|100000|9.3602 s|±2.9405|15.2063|

*Centroids initialization:*

| Number of samples | Average | Confidence | Variance |
| :---- | :----: | :----: | :----: |
|1000|4.4854 s|±1.2927|2.9390|
|10000|4.1256 s|±1.0579|1.9684|
|100000|4.7869 s|±0.9388|1.5500|

![comparison](/doc/img/7_7.png)

**Datasets with dimension = 7 and k = 13**

## 6. Credits
