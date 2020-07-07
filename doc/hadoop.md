# K-Means - Hadoop implementation

## Table of Contents
1) [Point](#1-point)
2) [Main](#2-main)
3) [Mapper](#3-mapper)
4) [Combine](#4-combiner)
5) [Reducer](#4-reducer)

## 1. Point
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
(*lines 81-86 of [Point.java](/k-means/src/main/java/it/unipi/hadoop/model/Point.java)*)


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
        dist = (float)Math.round(Math.pow(dist, 1f/h*100000)/100000.0f;
        return dist;
    }
}
```
(*lines 83-115 of [Point.java](/k-means/src/main/java/it/unipi/hadoop/model/Point.java)*)


The centroid is the arithmetic mean position of the points of a cluster. The average function computes the centroid from a sum of points.

```java
public void average() {
    for (int i = 0; i < this.dim; i++) {
        float temp = this.components[i] / this.numPoints;
        this.components[i] = (float)Math.round(temp*100000)/100000.0f;
    }
    this.numPoints = 1;
}
```
(*lines 117-123 of [Point.java](/k-means/src/main/java/it/unipi/hadoop/model/Point.java)*)

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
(*lines 49-57 of [Point.java](/k-means/src/main/java/it/unipi/hadoop/model/Point.java)*)

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
(*lines 60-67 of [Point.java](/k-means/src/main/java/it/unipi/hadoop/model/Point.java)*)

### 2. Main

#### 2.1 Parameters


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


#### 2.2 Centroids Initialization
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
(*lines 51-60 of [KMeans.java](/k-means/src/main/java/it/unipi/hadoop/KMeans.java)*)


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
(*lines 69-81 of [KMeans.java](/k-means/src/main/java/it/unipi/hadoop/KMeans.java)*)

#### 2.3 Main loop
The program is a MapReduce workflow. At the beginning all the parameters are set and centroids are initialized. For each iteration of the algorithm we configure a new job and wait for its completion. If the job fails, the program is killed. If the job succeed, the centroids are stored in the hdfs. To check the stopping criterion we read them to compare with previous centroids. The files need to be destroied to let the next job, if any, to write the result in the same path. If the stopping condition holds, the centroids are saved in a unique text file.

Since the computation of the centroids can be parallelized, we decided to set the number of reducers equals to the number of centroids so that each reducer computes a centroid. 

```java
iteration.setNumReduceTasks(K); 
```
(*line 174 of [KMeans.java](/k-means/src/main/java/it/unipi/hadoop/KMeans.java)*)

### 3. Mapper
The number of centrois, the centroids and the type of distance to use are retreived from the configuration file.

```java
public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {

    private Point[] centroids;
    private int p;
    private final Point point = new Point();
    private final IntWritable centroid = new IntWritable();

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
        point.set(pointString);

        // Initialize variables
        float minDist = Float.POSITIVE_INFINITY;
        float distance = 0.0f;
        int nearest = -1;

        // Find the closest centroid
        for (int i = 0; i < centroids.length; i++) {
            distance = point.distance(centroids[i], p);
            if(distance < minDist) {
                nearest = i;
                minDist = distance;
            }
        }

        centroid.set(nearest);
        context.write(centroid, point);
    }
}
```
(*lines 12-54 of [KMeansMapper.java](/k-means/src/main/java/it/unipi/hadoop/mapreduce/KMeansMapper.java)*)

### 4. Combiner
The combiner is an optimization, so it is not guaranteed to run. The input and output of the combiner needs to be identical, because needs to match with the output of the mapper and the input of the reducer. Our combiner performs a partial sum of the point emitted by a mapper to reduce the amount of data transmitted over the network.

```java
public class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {

    public void reduce(IntWritable centroid, Iterable<Point> points, Context context) 
        throws IOException, InterruptedException {

        //Sum the points
        Point sum = Point.copy(points.iterator().next());
        while (points.iterator().hasNext()) {
            sum.sum(points.iterator().next());
        }
        
        context.write(centroid, sum);
    }
}
```
(*lines 11-24 of [KMeansCombiner.java](/k-means/src/main/java/it/unipi/hadoop/mapreduce/KMeansCombiner.java)*)

### 5. Reducer
The reducer sums the points and compute the centroid as the average point. The result is the same whether the combiner will run or not.

```java
public class KMeansReducer extends Reducer<IntWritable, Point, Text, Text> {

    private final Text centroidId = new Text();
    private final Text centroidValue = new Text();
    
    public void reduce(IntWritable centroid, Iterable<Point> partialSums, Context context)
        throws IOException, InterruptedException {
        
        //Sum the partial sums
        Point sum = Point.copy(partialSums.iterator().next());
        while (partialSums.iterator().hasNext()) {
            sum.sum(partialSums.iterator().next());
        }
        //Calculate the new centroid
        sum.average();
        
        centroidId.set(centroid.toString());
        centroidValue.set(sum.toString());
        context.write(centroidId, centroidValue);
    }
}
```
(*lines 11-31 of [KMeansReducer.java](/k-means/src/main/java/it/unipi/hadoop/mapreduce/KMeansReducer.java)*)