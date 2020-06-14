package it.unipi.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Point implements Writable {
    
    private float[] components = null;
    private int dim;
    private int numPoints = 1;      //for partial sum

    public Point() {
        this.dim = 0;
    }
    
    public Point(final float[] c) {
        this.set(c);
    }

    public static Point copy(final Point p) {
        return new Point(p.components);
    }
    
    public void set(final float[] c) {
        this.components = c;
        this.dim = c.length;
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.dim = in.readInt();
        this.numPoints = in.readInt();
        this.components = new float[dim];

        for(int i = 0; i < dim; i++) {
            this.components[i] = in.readFloat();
        }
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.dim);
        out.writeInt(this.numPoints);

        for(int i = 0; i < dim; i++) {
            out.writeFloat(this.components[i]);
        }
    }

    @Override
    public String toString() {
        StringBuilder point = new StringBuilder();

        for (int i = 0; i < dim; i++) {
            point.append(Float.toString(this.components[i]));
            if(i != dim - 1) {
                point.append(",");
            }   
        }

        return point.toString();
    }

    public void sum(Point p) {
        float[] sum = new float[dim];
        for (int i = 0; i < dim; i++) {
            sum[i] = this.components[i] + p.components[i];
        }

        this.numPoints += p.numPoints;
    }

    public float distance(Point p, int h){
        if (h == 0)
            return -1;

        if (h == Integer.MAX_VALUE) {
            // Chebyshev distance
            float max = -1f;
            float diff = 0.0f;
            for (int i = 0; i < dim; i++) {
                diff = Math.abs(this.components[i] - p.components[i]);
                if (diff > max)              
                    max = diff;
            }
            return max;
        } else {
            // p-norm (sum |x_i-y_i|^p)^1/p
            float dist = 0.0f;
            for (int i = 0; i < dim; i++) {
                dist += Math.pow(Math.abs(this.components[i] - p.components[i]), h);
            }
            dist = (float) Math.pow(dist, 1f/h);
            return dist;
        }
    }

    public Point getAveragePoint() {
        Point averagePoint = new Point(this.components);
        for (int i = 0; i < this.dim; i++) {
            averagePoint.components[i] /= this.numPoints; 
        }
        return averagePoint;
    }

}