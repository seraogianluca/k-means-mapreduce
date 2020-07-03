package it.unipi.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import it.unipi.hadoop.model.Point;

public class KMeansReducer extends Reducer<IntWritable, Point, Text, Text> {

    private Text centroidId = new Text();
    private Text centroidValue = new Text();
    
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