package it.unipi.hadoop.util;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;

public class FloatArrayWritable extends ArrayWritable {

    public FloatArrayWritable() {
        super(FloatWritable.class);
    }

    public FloatArrayWritable(final FloatWritable[] values) {
        super(FloatWritable.class, values);
    }

    @Override
    public FloatWritable[] get() {
        return (FloatWritable[]) super.get();
    }

    public float getValue(final int i) {
        return this.toArray()[i];
    }

    @Override
    public float[] toArray() {
        return (float[])super.toArray();
    }

}