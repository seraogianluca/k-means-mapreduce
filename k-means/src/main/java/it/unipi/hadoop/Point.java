package it.unipi.hadoop;

import java.io.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Writable.FloatArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class Point implements Writable {
    
    private FloatArrayWritable components;

    public Point() {}
    
    public Point(final float[] c) {
        this.set(c);
    }

    public static copy(final Point p) {
        return new Point(p.components);
    }
    
    public void set(final float[] c) {
        if (c != null) {
            this.components = new FloatArrayWritable(c);
        }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.components = in.readFields(in);
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.write(components);
    }

    @Override
    public String toString() {
        int size = this.components.size;
        StringBuilder point = new StringBuilder();
        point.append("("); 
        for (int i = 0; i < size; i++) {
            point.append(p.components[i].toString());
            point.append(",");
        }
        point.append(")");
        return point.toString();
    }

    public Point sum(Point p) {
        int size = p.components.size;
        Point sum = new Point(new float[size]);
        for (int i = 0; i < size; i++) {
            sum.components[i] = this.components[i] + p.components[i];
        }
        return sum;
    }
    
    public Point difference(Point p){
        int size = p.components.size;
        Point  dif = new Point(new float[size]);
        for (int i = 0; i < size; i++) {
            dif.components[i]= this.components[i] - p.components[i];
        }
        return dif;        
    }

    public float distance(Point p, int h){
        if (h == 0)
            return -1;
        
        int size = p.components.size;
        if (h == Integer.POSITIVE_INFINITY) {
            // Chebyshev distance
            float max = -1f;
            float diff = 0.0f;
            for (int i = 0; i < size; i++) {
                diff = Math.abs(this.components[i] - p.components[i]);
                if (diff > max)
                    max = diff;
            }
            return max;
            
        } else {
            // p-norm (sum |x_i-y_i|^p)^1/p
            float dist = 0.0f;
            for (int i = 0; i < size; i++) {
                dist += Math.pow(Math.abs(this.components[i] - p.components[i]), h);
            }
            dist = Math.pow(dist, 1/h);
            return dist;
        }
    }
}