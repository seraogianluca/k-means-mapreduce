package it.unipi.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.unipi.hadoop.model.Point;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {

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

