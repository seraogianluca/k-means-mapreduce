package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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

    private static boolean stoppingCriterion(Point[] oldCentroids, Point[] newCentroids, int distance, float threshold) {
        boolean check = true;
        for(int i = 0; i < oldCentroids.length; i++) {
            check = oldCentroids[i].distance(newCentroids[i], distance) <= threshold;
            if (!check) {
                return false;
            }
        }
        return true;
    }

    private static Point[] centroidsInit(Configuration conf, String pathString, int k, int dataSetSize) 
      throws IOException {
    	Point[] points = new Point[k];
    	
        // Create a sorted list of positions without duplicates
        // Positions are the line index of the random selected centroids
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
        
        // File reading utils
        Path path = new Path(pathString);
    	FileSystem hdfs = FileSystem.get(conf);
    	FSDataInputStream in = hdfs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        // Get centroids from the file
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
        
    	return points;
    } 

    private static Point[] readCentroids(Configuration conf, int k, String pathString) throws IOException {
        Point[] points = new Point[k];
        FileSystem hdfs = FileSystem.get(conf);	
        
        for (int i = 0; i < k; i++) {
            Path path = new Path(pathString + "/part-r-0000" + i);
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)));
            
            String[] keyValueSplit = br.readLine().split("\t"); //Split line in K,V
            int centroidId = Integer.parseInt(keyValueSplit[0]);
            String[] point = keyValueSplit[1].split(",");
            points[centroidId] = new Point(point);
            br.close();
        }
        hdfs.delete(new Path(pathString), true); //delete temp directory

    	return points;
    }

    private static void finalize(Configuration conf, Point[] centroids, String output) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream dos = hdfs.create(new Path(output + "/centroids.txt"), true);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));

        for(int i = 0; i < centroids.length; i++) {
            br.write(centroids[i].toString());
            br.newLine();
        }

        br.close();
        hdfs.close();
    }

    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        conf.addResource(new Path("config.xml")); //Configuration file for the parameters

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: <input> <output>");
            System.exit(1);
        }

        // Parameters setting
        final String INPUT = otherArgs[0];
        final String OUTPUT = otherArgs[1] + "/temp";
        final int DATASET_SIZE = conf.getInt("dataset", 10);
        final int DISTANCE = conf.getInt("distance", 2);
        final int K = conf.getInt("k", 3);
        final float THRESHOLD = conf.getFloat("threshold", 0.0001f);
        final int MAX_ITERATIONS = conf.getInt("max.iteration", 30);

        Point[] oldCentroids = new Point[K];
        Point[] newCentroids = new Point[K];

        // Initial centroids
        newCentroids = centroidsInit(conf, INPUT, K, DATASET_SIZE);
        for(int i = 0; i < K; i++) {
            conf.set("centroid." + i, newCentroids[i].toString());
        }

        // MapReduce workflow
        boolean stop = false;
        boolean succeded = true;
        int i = 0;
        while(!stop) {
            i++;

            // Job configuration
            Job iteration = Job.getInstance(conf, "iter_" + i);
            iteration.setJarByClass(KMeans.class);
            iteration.setMapperClass(KMeansMapper.class);
            iteration.setCombinerClass(KMeansCombiner.class);
            iteration.setReducerClass(KMeansReducer.class);  
            iteration.setNumReduceTasks(K); //one task each centroid            
            iteration.setOutputKeyClass(IntWritable.class);
            iteration.setOutputValueClass(Point.class);
            FileInputFormat.addInputPath(iteration, new Path(INPUT));
            FileOutputFormat.setOutputPath(iteration, new Path(OUTPUT));
            iteration.setInputFormatClass(TextInputFormat.class);
            iteration.setOutputFormatClass(TextOutputFormat.class);

            succeded = iteration.waitForCompletion(true);

            if(!succeded) { // If the job fails the application will be closed.       
                System.err.println("Iteration" + i + "failed.");
                System.exit(1);
            }

            // Save old centroids and read new centroids
            for(int id = 0; id < K; id++) {
                oldCentroids[id] = Point.copy(newCentroids[id]);
            }                        
            newCentroids = readCentroids(conf, K, OUTPUT);

            stop = stoppingCriterion(oldCentroids, newCentroids, DISTANCE, THRESHOLD);

            if(stop || i == MAX_ITERATIONS) {
                finalize(conf, newCentroids, otherArgs[1]);
            } else {
                // Set the new centroids in the configuration
                for(int d = 0; d < K; d++) {
                    conf.unset("centroid." + d);
                    conf.set("centroid." + d, newCentroids[d].toString());
                }
            }
        }

        System.exit(0);
    }

}