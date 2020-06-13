package it.unipi.hadoop.model;

import it.unipi.hadoop.util.FloatArrayWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.FloatWritable;

public class Point implements Writable {
    
    private FloatArrayWritable components = null;
    private int dim;
    private int numPoints = 1;      //for partial sum

    public Point() {
        this.dim = 0;
    }
    
    public Point(final FloatWritable[] c) {
        this.set(c);
        this.dim = c.length;
    }


    public Point(final float[] c) {
        FloatWritable[] fWritables = new FloatWritable[c.length];
        for (int i = 0; i < fWritables.length; i++) {
            fWritables[i] = new FloatWritable(c[i]);
        }
        this.set(fWritables);
        this.dim = c.length;
    }

    public static Point copy(final Point p) {
        return new Point(p.components.get());
    }
    
    public void set(final FloatWritable[] c) {
        if(this.components == null) {
            this.components = new FloatArrayWritable(c);
        } else {
            this.components.set(c);
            this.dim = c.length;
        }
    }

    public void setNumberOfPoints(int np) {
        this.numPoints = np;
    }

    public int getNumberOfPoints() {
        return this.numPoints;
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.components.readFields(in);
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        this.components.write(out);
    }

    @Override
    public String toString() {
        String[] values = this.components.toStrings();
        StringBuilder point = new StringBuilder(); 
        for (int i = 0; i < dim; i++) {
            point.append(values[i]);
            point.append(",");
        }
        return point.toString();
    }

    public Point sum(Point p) {
        FloatWritable[] sum = new FloatWritable[dim];
        for (int i = 0; i < dim; i++) {
            sum[i] = new FloatWritable(this.components.getValue(i) + p.components.getValue(i));
        }
        return new Point(sum);
    }
    
    public Point difference(Point p){  //tra centroidi
        FloatWritable[] diff = new FloatWritable[dim];
        for (int i = 0; i < dim; i++) {
            diff[i] = new FloatWritable(this.components.getValue(i) - p.components.getValue(i));
        }
        return new Point(diff);        
    }

    public float distance(Point p, int h){
        if (h == 0)
            return -1;

        if (h == Integer.MAX_VALUE) {
            // Chebyshev distance
            float max = -1f;
            float diff = 0.0f;
            for (int i = 0; i < dim; i++) {
                diff = Math.abs(this.components.getValue(i) - p.components.getValue(i));
                if (diff > max)              
                    max = diff;
            }
            return max;
        } else {
            // p-norm (sum |x_i-y_i|^p)^1/p
            float dist = 0.0f;
            for (int i = 0; i < dim; i++) {
                dist += Math.pow(Math.abs(this.components.getValue(i) - p.components.getValue(i)), h);
            }
            dist = (float) Math.pow(dist, 1f/h);
            return dist;
        }
    }

    public Point getAveragePoint() {
        Point averagePoint = new Point(this.components.get());
        for (int i = 0; i < this.dim; i++) {
            averagePoint.components.get()[i].set(this.components.getValue(i)/this.numPoints); 
        }
        return averagePoint;
    }

}