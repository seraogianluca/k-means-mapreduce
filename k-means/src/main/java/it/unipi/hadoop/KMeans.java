package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import it.unipi.hadoop.model.Point;

public class KMeans {

    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {

        private Point[] centroids = new Point[10];
        private int p; 

        public void setup(Context context) {
            // TODO: Creation of array of centroids
            // TODO: initialize the type of distance to use

            this.p = 2;
        }

        public void map(LongWritable key, Text value, Context context) 
         throws IOException, InterruptedException {
            
            // Contruct the point
            String[] compString = value.toString().split(",");
            float[] compFloat = new float[compString.length];
            for (int i = 0; i < compString.length; i++) {
                compFloat[i] = Float.parseFloat(compString[i]);
            }
            Point point = new Point(compFloat);
            
            //Initialize variables
            float min_dist = Float.POSITIVE_INFINITY;
            float distance = 0.0f;
            IntWritable centroid = new IntWritable(-1);

            //Find the closest centroid
            for (int i = 0; i < centroids.length; i++) {
                distance = point.distance(centroids[i], p);
                if(distance < min_dist){
                    centroid.set(i);
                    min_dist = distance;
                }
            }

            context.write(centroid, point);
            
        }

    }



    public static class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {
    
        public void reduce(IntWritable centroid, Iterable<Point> points, Context context) 
         throws IOException, InterruptedException {

            Point sum = Point.copy(points.iterator().next());
            int numPoints = 1;

            while (points.iterator().hasNext()) {
                sum.sum(points.iterator().next());
                numPoints++;
            }

            sum.setNumberOfPoints(numPoints);
            context.write(centroid, sum);  
        }
    }

/*The #1 rule of Combiners are: do not assume that the combiner will run. 
Treat the combiner only as an optimization.

The Combiner is not guaranteed to run over all of your data. 
In some cases when the data doesn't need to be spilled to disk, MapReduce will skip using the Combiner entirely. 
Note also that the Combiner may be ran multiple times over subsets of the data! It'll run once per spill.

In your case, you are making this bad assumption. 
You should be doing the sum in the Combiner AND the Reducer.

The input and output of the combiner needs to be identical (Text,Double -> Text,Double) 
and it needs to match up with the output of the Mapper and the input of the Reducer.


Unlike a Reducer, input/output key and value types of combiner must match the output types of your Mapper .

Combiners can only be used on the functions that are commutative (a.b = b.a) and associative {a.(b.c) = (a.b).c} .
From this, we can say that combiner may operate only on a subset of your keys and values. Or may does not execute at all, 
still, you want the output of the program to remain same.
 
From multiple Mappers, Reducer get its input data as part of the partitioning process. 
Combiners can only get its input from one Mapper.
*/


    public static void main(String[] args) {
        
    }

}