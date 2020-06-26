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

### 3.1 Point

The point is the basic object of our implementation on which all the operations are executed. The point represents both a single point, that can be a data point or a centroid, and a sum of points, that is used in the combiner and the reducer. The point class implements the hadoop writable interface because it needs to be serialized. Besides it implements all the operations required by the algorithm, such as the sum between points, the distance between points and the centroid calculation.

The point class has the fields:
- `dim` that is the dimension of the point.
- `components` that are the features of the point.
- `numPoints` that is 1 in case of a single point, otherwise is the number of summed points, this number will be used in the reducer to compute the centroid.

```java
private float[] components = null;
private int dim;
private int numPoints;
```
(*lines 11-13 of [Point.java](/k-means/src/main/java/it/unipi/hadoop/model/Point.java)*)

To sum two points or partial sum of points we use the same function. `numPoints` keeps track of how many points compose the sum.

```java
public void sum(Point p) {
    for (int i = 0; i < this.dim; i++) {
        this.components[i] += p.components[i];
    }
    this.numPoints += p.numPoints;
}
```
(*lines 76-81 of [Point.java](/k-means/src/main/java/it/unipi/hadoop/model/Point.java)*)


Depending on the value of `h` is possible to compute different types of distances. In particular, the Chebyshev ( *h = 0* ), the Manhattan ( *h = 1* ), the Euclidean ( *h = 2* ) and the Minkowski ( *h > 2* ) distances. 

```java
public float distance(Point p, int h){
    if (h < 0) {
        // Consider only metric distances
        h = 2;   
    }
    
    if (h == 0) {
        // Chebyshev
        float max = -1f;
        float diff = 0.0f;
        for (int i = 0; i < this.dim; i++) {
            diff = Math.abs(this.components[i] - p.components[i]);
            if (diff > max) {
                max = diff;
            }                       
        }
        return max;

    } else {
        // Manhattan, Euclidean, Minkowsky
        float dist = 0.0f;
        for (int i = 0; i < this.dim; i++) {
            dist += Math.pow(Math.abs(this.components[i] - p.components[i]), h);
        }
        dist = (float)Math.pow(dist, 1f/h);
        return dist;
    }
}
```
(*lines 83-110 of [Point.java](/k-means/src/main/java/it/unipi/hadoop/model/Point.java)*)


The centroid is the arithmetic mean position of the points of a cluster. The average function computes the centroid from a sum of points.

```java
public void average() {
    for (int i = 0; i < this.dim; i++) {
        this.components[i] /= this.numPoints;
    }
    this.numPoints = 1;
}
```
(*lines 112-118 of [Point.java](/k-means/src/main/java/it/unipi/hadoop/model/Point.java)*)

The `readFields` and `write` functions perform a field by field de-serialization and serialization of the point respectively.

```java
@Override
public void readFields(final DataInput in) throws IOException {
    this.dim = in.readInt();
    this.numPoints = in.readInt();
    this.components = new float[this.dim];

    for(int i = 0; i < this.dim; i++) {
        this.components[i] = in.readFloat();
    }
}
```
(*lines 43-52 of [Point.java](/k-means/src/main/java/it/unipi/hadoop/model/Point.java)*)

```java
@Override
public void write(final DataOutput out) throws IOException {
    out.writeInt(this.dim);
    out.writeInt(this.numPoints);

    for(int i = 0; i < this.dim; i++) {
        out.writeFloat(this.components[i]);
    }
}

```
(*lines 54-62 of [Point.java](/k-means/src/main/java/it/unipi/hadoop/model/Point.java)*)

### 3.2 Main

#### 3.2.1 Parameters


Our k-means has the following parameters:
- `DATASET_SIZE` is the number of points in the dataset. Points are line of comma separated values in a text file.  
- `DISTANCE` is the type of distance to use.
- `K` is the number of partition to find.  
- `THRESHOLD` is the threshold for the stopping criterion. 
- `MAX_ITERATIONS` is the maximum number of iteration and is used as additional stopping criterion.

These parameters are loaded from a config.xml file stored in the hdfs. Instead, the input and output path are passed through command line.

```xml
<property>
    <name>distance</name>
    <value>5</value>
    <description>Type of distance adopted</description>
</property>
```
(*lines 8-12 of [config.xml](/k-means/config.xml)*)


#### 3.2.2 Centroids Initialization
At the first iteration, the centroids are randomly sampled from the dataset. Since the reading operation could be an heavy task in case of large datasets, we optimized it so that file is scanned at most once.

First, the index of the centroids are randomly generated and saved in a sorted list. The index corresponds to the line of the dataset file.

```java
List<Integer> positions = new ArrayList<Integer>();
Random random = new Random();
int pos;
while(positions.size() < k) {
    pos = random.nextInt(dataSetSize);
    if(!positions.contains(pos)) {
        positions.add(pos);
    }
}
Collections.sort(positions);
```
(*lines 118-127 of [KMeans.java](/k-means/src/main/java/it/unipi/hadoop/model/KMeans.java)*)


The file is scanned to retrieve the value of the centroids. The scan ends at the line on which the last centroid lies.

```java
int row = 0;
int i = 0;
int position;
while(i < positions.size()) {
    position = positions.get(i);
    String point = br.readLine();
    if(row == position) {    
        points[i] = new Point(point.split(","));  
        i++;
    }
    row++;
}   
br.close();
```
(*lines 136-147 of [KMeans.java](/k-means/src/main/java/it/unipi/hadoop/model/KMeans.java)*)



--------------------------- 
# !!!! TO DO !!!!

abbiamo testato su un file da 100000 e 1m e il tempo per il constesto in cui siamo era accettabile quindi stoppe. 
si poteva fare di piu? sicuramente! avevamo lo sbatti? assolutamente no.
VEDIAMO SE TOCCA IMPLEMENTA MAPREDUCE ANCHE PER LA RANDOM CHOICE OF CENTROIDS

--------------------------



#### 3.2.3 Main loop
The program is a MapReduce workflow. At the beginning all the parameters are set and centroids are initialized. For each iteration of the algorithm we configure a new job and wait for its completion. If the job fails, the program is killed. If the job succeed, the centroids are stored in the hdfs. To check the stopping criterion we read them to compare with previous centroids. The files need to be destroied to let the next job, if any, to write the result in the same path. If the stopping condition holds, the centroids are saved in a unique text file.

Since the computation of the centroids can be parallelized, we decided to set the number of reducers equals to the number of centroids so that each reducer computes a centroid. 

```java
iteration.setNumReduceTasks(K); //one task each centroid 
```
(*line 229 of [KMeans.java](/k-means/src/main/java/it/unipi/hadoop/model/KMeans.java)*)

### 3.3 Mapper
The number of centrois, the centroids and the type of distance to use are retreived from the configuration file.

```java
    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {

        private Point[] centroids;
        private int p;

        public void setup(Context context) {
            int k = Integer.parseInt(context.getConfiguration().get("k"));
            this.p = Integer.parseInt(context.getConfiguration().get("distance"));

            this.centroids = new Point[k];
            for(int i = 0; i < k; i++) {
                String[] centroid = context.getConfiguration().getStrings("centroid." + i);
                this.centroids[i] = new Point(centroid);
            }
        }

        public void map(LongWritable key, Text value, Context context) 
         throws IOException, InterruptedException {
            
            // Contruct the point
            String[] pointString = value.toString().split(",");
            Point point = new Point(pointString);

            // Initialize variables
            float minDist = Float.POSITIVE_INFINITY;
            float distance = 0.0f;
            IntWritable centroid = new IntWritable(-1);

            // Find the closest centroid
            for (int i = 0; i < centroids.length; i++) {
                distance = point.distance(centroids[i], p);
                if(distance < minDist) {
                    centroid.set(i);
                    minDist = distance;
                }
            }
            context.write(centroid, point);
        }
    }
```
(*lines 35-73 of [KMeans.java](/k-means/src/main/java/it/unipi/hadoop/model/KMeans.java)*)

### 3.4 Combiner
The combiner is an optimization, so it is not guaranteed to run. The input and output of the combiner needs to be identical, because needs to match with the output of the mapper and the input of the reducer. Our combiner performs a partial sum of the point emitted by a mapper to reduce the amount of data transmitted over the network.

```java
public static class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {

    public void reduce(IntWritable centroid, Iterable<Point> points, Context context) 
        throws IOException, InterruptedException {
        Point sum = Point.copy(points.iterator().next());
        while (points.iterator().hasNext()) {
            sum.sum(points.iterator().next());
        }
        context.write(centroid, sum);
    }
}
```
(*lines 75-85 of [KMeans.java](/k-means/src/main/java/it/unipi/hadoop/model/KMeans.java)*)

### 3.5 Reducer
The reducer sums the points and compute the centroid as the average point. The result is the same whether the combiner will run or not.

```java
public static class KMeansReducer extends Reducer<IntWritable, Point, Text, Text> {

    public void reduce(IntWritable centroid, Iterable<Point> partialSums, Context context)
        throws IOException, InterruptedException {
        Point sum = Point.copy(partialSums.iterator().next());
        while (partialSums.iterator().hasNext()) {
            sum.sum(partialSums.iterator().next());
        }
        //Calculate the new centroid
        sum.average();
        context.write(new Text(centroid.toString()), new Text(sum.toString()));
    }
}
```
(*lines 87-99 of [KMeans.java](/k-means/src/main/java/it/unipi/hadoop/model/KMeans.java)*)

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



