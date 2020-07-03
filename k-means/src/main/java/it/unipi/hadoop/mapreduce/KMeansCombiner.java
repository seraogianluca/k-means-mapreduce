
package it.unipi.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import it.unipi.hadoop.model.Point;

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