# The k-means clustering algorithm using MapReduce

// k # of clusters\
// X = {x1, ...., xn}   n = # points in d dimensions\
// M = {u_1, ..., u_k}  means (centroids) in d dimensions, randomly sampled from X\
// d = 3  -> x1 = [x1_1, x1_2, x1_3], x2 = ..., xn =... \ 

<!-- Program -->
//K-Means(X,k)


# Mapper
**Input:** (Offset, Point)

**Map function**

```
x = Construct the Point
min_dist = POSITIVE_INFINITY
c = -1 (Closest centroid)
for (centroid in centroids) {
    dist = distance(centroid, x)
    if(dist < min_dist) {
        c = index_of(centroid)
        min_dist = dist
    }
}
emit(c, x)
```
 
**Output:** (Closest centroid index c_j, x_i)

# Combiner
**Input:** (Centroid_id, List of points)

**Combine function**

```
number_of_points = 0
sum = 0
for each point in list of points:
    sum += point
    number_of_points += 1

average = sum / num
```

//CALCOLO ERRATO

**Output:** (Centroid_id, average)

# Reducer
**Input:** (Centroid_id, List of points)

**Reduce function**

```
number_of_point = 0
sum = 0
for each point in list of points:
    sum += point
    number_of_points += 1

centroid_new_value = sum / number_of_points
```

**Output:** (Centroid_id, centroid_new_value)

# File di input
(3, 2, 1)\
(3, 2, 0)\
(2, 0, 2)\
(6, 3, 4)
