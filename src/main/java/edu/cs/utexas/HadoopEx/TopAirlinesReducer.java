package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopAirlinesReducer extends Reducer<Text, DelayCountWritable, Text, DoubleWritable> {
    private TreeMap<Double, Text> topAirlines = new TreeMap<>(Collections.reverseOrder());
    private DoubleWritable ratio = new DoubleWritable();

    @Override
    public void reduce(Text key, Iterable<DelayCountWritable> values, Context context) {
        double totalDelay = 0;
        int totalFlights = 0;
        for (DelayCountWritable val : values) {
            totalDelay += val.getSumDelay();
            totalFlights += val.getCount();
        }
        if (totalFlights == 0) return; // Avoid division by zero

        double delayRatio = totalDelay / totalFlights;
        topAirlines.put(delayRatio, new Text(key));

        // Keep only the top 3 entries
        if (topAirlines.size() > 3) {
            topAirlines.pollLastEntry();
        }
    }

    @Override
    protected void cleanup(Context context) 
            throws IOException, InterruptedException {
        // Emit top 3 airlines
        for (Map.Entry<Double, Text> entry : topAirlines.entrySet()) {
            ratio.set(entry.getKey());
            context.write(entry.getValue(), ratio);
        }
    }
}