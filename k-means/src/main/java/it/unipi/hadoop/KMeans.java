package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


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

/*
 Reducer
**Input:** (Centroid_id, List of partial_sums)

**Reduce function**
```
number_of_point = 0
sum = 0
for each point in list of points:
    sum += point
    number_of_points += 1

centroid_new_value = sum / number_of_points
```

**Output:** (Centroid_id, centroid_new_value)*/


    public static class KMeansReducer extends Reducer<IntWritable, Point, IntWritable, Point >{

        public void reduce(IntWritable centroid, Iterable<Point> partialSum, Context context)
            throws IOException, InterruptedException{

            // Sum
            Point sum = Point.copy(partialSum.iterator().next());
            int numPoints = sum.getNumberOfPoints();

            while (partialSum.iterator().hasNext()) {
                Point p = partialSum.iterator().next();
                sum.sum(p);
                numPoints += p.getNumberOfPoints();
            }
            sum.setNumberOfPoints(numPoints);
            //Average, recalculate centroid
            Point newCentroidValue = sum.getAveragePoint();

            // TODO: save shared centroid

            context.write(centroid, newCentroidValue);

        }
    }


    
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: K-Means <number_of_clusters> <threshold> <input> <output>");
            System.exit(1);
        }
        System.out.println("args[0]: number_of_cluster="  + otherArgs[0]);
        System.out.println("args[1]: threshold=" + otherArgs[1]);
        System.out.println("args[2]: <input>="  + otherArgs[2]);
        System.out.println("args[3]: <output>=" + otherArgs[3]);

        // TODO: mapreduce stages (while)
        // TODO: initial centroid selections

        Job job = Job.getInstance(conf, "KMeans");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class);
        job.setReducerClass(KMeansReducer.class);
        
        //one task each centroid
        job.setNumReduceTasks(Integer.parseInt(otherArgs[0]));

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Point.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}