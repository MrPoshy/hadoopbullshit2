package edu.cs.utexas.HadoopEx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class DelayCountWritable implements Writable {
    private double sumDelay;
    private int count;

    public DelayCountWritable() {} // Default constructor

    public DelayCountWritable(double sumDelay, int count) {
        this.sumDelay = sumDelay;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sumDelay);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sumDelay = in.readDouble();
        count = in.readInt();
    }

    // Getters
    public double getSumDelay() { return sumDelay; }
    public int getCount() { return count; }
}