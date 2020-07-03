package it.unipi.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Point implements Writable {
    
    private float[] components = null;
    private int dim;
    private int numPoints; // For partial sums

    public Point() {
        this.dim = 0;
    }
    
    public Point(final float[] c) {
        this.set(c);
    }

    public Point(final String[] s) {  
        float[] comp = new float[s.length];
        for (int k = 0; k < s.length; k++) {
            comp[k] = Float.parseFloat(s[k]);
        }
        this.set(comp);
    }

    public static Point copy(final Point p) {
        Point ret = new Point(p.components);
        ret.numPoints = p.numPoints;
        return ret;
    }
    
    public void set(final float[] c) {
        this.components = c;
        this.dim = c.length;
        this.numPoints = 1;
    }

    public void set(final String[] s) {
        float[] comp = new float[s.length];
        for (int k = 0; k < s.length; k++) {
            comp[k] = Float.parseFloat(s[k]);
        }
        this.set(comp);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.dim = in.readInt();
        this.numPoints = in.readInt();
        this.components = new float[this.dim];

        for(int i = 0; i < this.dim; i++) {
            this.components[i] = in.readFloat();
        }
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.dim);
        out.writeInt(this.numPoints);

        for(int i = 0; i < this.dim; i++) {
            out.writeFloat(this.components[i]);
        }
    }

    @Override
    public String toString() {
        StringBuilder point = new StringBuilder();
        for (int i = 0; i < this.dim; i++) {
            point.append(Float.toString(this.components[i]));
            if(i != dim - 1) {
                point.append(",");
            }   
        }
        return point.toString();
    }

    public void sum(Point p) {
        for (int i = 0; i < this.dim; i++) {
            this.components[i] += p.components[i];
        }
        this.numPoints += p.numPoints;
    }

    public float distance(Point p, int h){
        if (h < 0) {
            // Consider only metric distances
            h = 2;   
        }
        
        if (h == 0) {
            // Chebyshev
            float max = -1f;
            float diff = 0.0f;
            for (int i = 0; i < this.dim; i++) {
                diff = Math.abs(this.components[i] - p.components[i]);
                if (diff > max) {
                    max = diff;
                }                       
            }
            return max;

        } else {
            // Manhattan, Euclidean, Minkowsky
            float dist = 0.0f;
            for (int i = 0; i < this.dim; i++) {
                dist += Math.pow(Math.abs(this.components[i] - p.components[i]), h);
            }
            dist = (float)Math.round(Math.pow(dist, 1f/h)*100000)/100000.0f;
            return dist;
        }
    }

    public void average() {
        for (int i = 0; i < this.dim; i++) {
            float temp = this.components[i] / this.numPoints;
            this.components[i] = (float)Math.round(temp*100000)/100000.0f;
        }
        this.numPoints = 1;
    }

    public static float frobeniusNorm(Point[] points) {
        float norm = 0.0f;
        for(int i = 0; i < points.length; i++) {
            for(int j = 0; j < points[i].dim; j++) {
                norm += Math.pow(points[i].components[j], 2);
            }
        }
        norm = (float)Math.round(Math.sqrt(norm)*100000)/100000.0f;
        return norm;
    }
}