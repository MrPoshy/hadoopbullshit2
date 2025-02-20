package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DelaySumCombiner extends Reducer<Text, DelayCountWritable, Text, DelayCountWritable> {
    private DelayCountWritable result = new DelayCountWritable();

    @Override
    public void reduce(Text key, Iterable<DelayCountWritable> values, Context context)
            throws IOException, InterruptedException {
        
        double sumDelay = 0;
        int sumCount = 0;
        for (DelayCountWritable val : values) {
            sumDelay += val.getSumDelay();
            sumCount += val.getCount();
        }
        result = new DelayCountWritable(sumDelay, sumCount);
        context.write(key, result);
    }
}