package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

        private Point[] centroids;
        private int p; 

        public void setup(Context context) {
            int k = Integer.parseInt(context.getConfiguration().get("k"));
            this.centroids = new Point[k];

            for(int i = 0; i < k; i++) {
                String centroid = context.getConfiguration().get("centroid." + i);
                String[] point = centroid.split(",");
                float[] components = new float[point.length];
                for (int j = 0; j < point.length; j++) {
                    components[j] = Float.parseFloat(point[j]);
                }

                this.centroids[i] = new Point(components);
            }

            this.p = Integer.parseInt(context.getConfiguration().get("p"));
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

    public static class KMeansReducer extends Reducer<IntWritable, Point, IntWritable, Text> {

        public void reduce(IntWritable centroid, Iterable<Point> partialSum, Context context)
            throws IOException, InterruptedException {

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

            context.write(centroid, new Text(newCentroidValue.toString()));
        }
    }

    private static boolean stoppingCriterion(Point[] oldCentroids, Point[] newCentroids, int distance, float treshold) {
        boolean check = true;

        for(int i = 0; i < oldCentroids.length; i++) {
            check = oldCentroids[i].distance(newCentroids[i], distance) < treshold;
            if (!check) {
                return false;
            }
        }

        return true;
    }

    private static Point[] centroidsInit(Configuration conf, String pathString, int dim, int centroids, int dataSetSize) 
      throws IOException {
    	Point[] points = new Point[centroids];
    	
    	// Create a set of position in order to avoid duplicates
    	Set<Integer> positionsSet = new HashSet<Integer>();
    	Random random = new Random();
    	while(positionsSet.size() < centroids) {
    		positionsSet.add(random.nextInt(dataSetSize));
    	}
    	List<Integer> positions = new ArrayList<Integer>(positionsSet);
    	
    	Path path = new Path(pathString);
    	FileSystem fs = FileSystem.get(conf);
    	FSDataInputStream in = fs.open(path);
        BufferedReader b = new BufferedReader(new InputStreamReader(in));
        for (int i = 0; i < points.length; i++) {
            String line = null;
            
            int position = positions.get(i);
            int j = 0;
            while((line = b.readLine()) == null || j < position) {
                j++;
            }
        
            String value = line;
            String[] compString = value.toString().split(",");
            float[] compFloat = new float[compString.length];
            for (int k = 0; k < compString.length; k++) {
                compFloat[k] = Float.parseFloat(compString[k]);
            }
            points[i] = new Point(compFloat);
        }
        b.close();
    	return points;
    } 

    public static Point[] getCentroids(Configuration conf, int dim, int centroids, String pathString) throws IOException {
        Point[] points = new Point[centroids];
        Path path = new Path(pathString);
    	FileSystem fs = FileSystem.get(conf);
    	FSDataInputStream in = fs.open(path);
        BufferedReader b = new BufferedReader(new InputStreamReader(in));
        
        for (int i = 0; i < centroids; i++) {
            String line = b.readLine();

            String[] splitForId = line.split(" ");
            int id = Integer.parseInt(splitForId[0]);

            String[] point = splitForId[1].split(",");
            float[] components = new float[dim];
            for (int k = 0; k < dim; k++) {
                components[k] = Float.parseFloat(point[k]);
            }
            points[id] = new Point(components);
        }

        b.close();
    	return points;
    }

    
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: KMeans <input> <output>");
            System.exit(1);
        }
        
        System.out.println("args[0]: input="  + otherArgs[0]);
        System.out.println("args[1]: output=" + otherArgs[1]);

        //settings, then by user
        int d = 3; // point dim  3 or 7
        int k = 7; // #centroids 7 or 13
        int dist = 2; //distance
        float t = 0.0001f; //treshold
        int dataSetSize = 10;

        conf.set("p", Integer.toString(dist));
        conf.set("k", Integer.toString(k));
        Point[] oldCentroids = new Point[k];
        Point[] newCentroids = new Point[k];

        newCentroids = centroidsInit(conf, otherArgs[0], d, k, dataSetSize);

        boolean stop = false;
        int i = 0;
        while(!stop) {
            i++;
            Job iteration = Job.getInstance(conf, "iteration " + i);
            iteration.setJarByClass(KMeans.class);
            iteration.setMapperClass(KMeansMapper.class);
            iteration.setCombinerClass(KMeansCombiner.class);
            iteration.setReducerClass(KMeansReducer.class);
            
            //one task each centroid
            iteration.setNumReduceTasks(Integer.parseInt(otherArgs[0]));

            iteration.setOutputKeyClass(IntWritable.class);
            iteration.setOutputValueClass(Point.class);

            FileInputFormat.addInputPath(iteration, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(iteration, new Path(otherArgs[1]));

            iteration.setInputFormatClass(TextInputFormat.class);
            iteration.setOutputFormatClass(TextOutputFormat.class);

            boolean succeded = iteration.waitForCompletion(true);

            //If the job fails the application will be closed.
            if(!succeded) {
                System.err.println("Error in the job.");
                System.exit(1);
            }

            for(int id = 0; id < newCentroids.length; id++) {
                oldCentroids[id] = Point.copy(newCentroids[id]);
            }
                        
            newCentroids = getCentroids(conf, d, k, otherArgs[1]);
            for(int j = 0; j < k; j++) {
                conf.set("centroid." + j, newCentroids.toString());
            }
            
            stop = KMeans.stoppingCriterion(oldCentroids, newCentroids, dist, t);
        }

        System.exit(0);
    }

}