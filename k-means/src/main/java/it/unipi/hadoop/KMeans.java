package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

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
            this.centroids = new Point[k];

            for(int i = 0; i < k; i++) {
                String centroid = context.getConfiguration().get("centroid." + i);
                String[] point = centroid.split(",");
                float[] components = new float[point.length];
                for (int j = 0; j < point.length; j++) {
                    components[j] = Float.parseFloat(point[j]);
                }

                this.centroids[i] = new Point(components);
                System.out.println(this.centroids[i].toString());
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
            System.out.println("centroid id:" + centroid.toString() + " punto di mario: " + point.toString());
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

    public static class KMeansReducer extends Reducer<Text, Point, Text, Text> {

        public void reduce(IntWritable centroid, Iterable<Point> partialSum, Context context)
            throws IOException, InterruptedException {

            // Sum
            Point sum = Point.copy(partialSum.iterator().next());

            while (partialSum.iterator().hasNext()) {
                sum.sum(partialSum.iterator().next());
            }

            //Average, recalculate centroid
            Point newCentroidValue = sum.getAveragePoint();

            context.write(new Text("centroid." + centroid), new Text(newCentroidValue.toString()));
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
        Collections.sort(positions);
        
        Path path = new Path(pathString);
    	FileSystem fs = FileSystem.get(conf);
    	FSDataInputStream in = fs.open(path);
        BufferedReader b = new BufferedReader(new InputStreamReader(in));

        int j = 0;
        int i = 0;
        int position = positions.get(0);
        while(i < positions.size()) {
            position = positions.get(i);

            if(j == position) {      
                String[] compString = b.readLine().split(",");
                float[] compFloat = new float[compString.length];
                for (int k = 0; k < compString.length; k++) {
                    compFloat[k] = Float.parseFloat(compString[k]);
                }
                points[i] = new Point(compFloat);
                i++;          
            }

            j++;
        }

        b.close();
    	return points;
    } 

    private static Point[] readCentroids(Configuration conf, int dim, int centroids, String pathString) throws IOException {
        Point[] points = new Point[centroids];
        FileSystem hdfs = FileSystem.get(conf);	
        
        for (int i = 0; i < centroids; i++) {
            Path path = new Path(pathString + "/part-r-0000" + i);
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)));
            String line = br.readLine();
            String[] splitForId = line.split("\t");
            int id = Integer.parseInt(splitForId[0]);
            String[] point = splitForId[1].split(",");
            float[] components = new float[dim];

            for (int k = 0; k < dim; k++) {
                components[k] = Float.parseFloat(point[k]);
            }

            points[id] = new Point(components);
            br.close();
        }

        hdfs.delete(new Path(pathString), true);

    	return points;
    }

    private static void setCentroids(Configuration conf, Point[] centroids) {
        for(int i = 0; i < centroids.length; i++) {
            conf.set("centroid." + i, centroids[i].toString());
        }
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
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 7) {
            System.err.println("Usage: <input> <output> <dataset size> <distance> <features> <k> <treshold>");
            System.exit(1);
        }

        final Path INPUT = new Path(otherArgs[0]);
        final Path OUTPUT = new Path(otherArgs[1] + "/temp");
        final int DATASET_SIZE = Integer.parseInt(otherArgs[2]);
        final int DISTANCE = Integer.parseInt(otherArgs[3]);
        final int DIM = Integer.parseInt(otherArgs[4]);
        final int K = Integer.parseInt(otherArgs[5]);
        final float TRESHOLD = Float.parseFloat(otherArgs[6]);

        conf.set("p", Integer.toString(DISTANCE));
        conf.set("k", Integer.toString(K));

        Point[] oldCentroids = new Point[K];
        Point[] newCentroids = new Point[K];

        newCentroids = centroidsInit(conf, otherArgs[0], DIM, K, DATASET_SIZE);
        setCentroids(conf, newCentroids);

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
            iteration.setNumReduceTasks(K);

            iteration.setOutputKeyClass(IntWritable.class);
            iteration.setOutputValueClass(Point.class);

            FileInputFormat.addInputPath(iteration, INPUT);
            FileOutputFormat.setOutputPath(iteration, OUTPUT);

            iteration.setInputFormatClass(TextInputFormat.class);
            iteration.setOutputFormatClass(TextOutputFormat.class);

            boolean succeded = iteration.waitForCompletion(true);

            if(!succeded) {
                //If the job fails the application will be closed.
                System.err.println("Job" + i + "failed.");
                System.exit(1);
            }

            for(int id = 0; id < K; id++) {
                oldCentroids[id] = Point.copy(newCentroids[id]);
            }
                        
            newCentroids = readCentroids(conf, DIM, K, otherArgs[1] + "/temp");
            stop = KMeans.stoppingCriterion(oldCentroids, newCentroids, DISTANCE, TRESHOLD);

            if(stop) {
                finalize(conf, newCentroids, otherArgs[1]);
            }
        }

        System.exit(0);
    }

}