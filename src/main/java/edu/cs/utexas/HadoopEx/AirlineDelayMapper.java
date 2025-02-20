package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirlineDelayMapper extends Mapper<Object, Text, Text, DelayCountWritable> {
    private Text airline = new Text();
    private DelayCountWritable delayCount = new DelayCountWritable();

    @Override
    public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String[] fields = value.toString().split(",", -1); // Handle empty fields
        if (fields[0].equals("YEAR")) return; // Skip header

        // Validate fields
        if (fields.length < 12 || fields[11].isEmpty()) return;

        // Extract airline (index 4) and delay (index 11)
        airline.set(fields[4]);
        try {
            double delay = Double.parseDouble(fields[11]);
            delayCount = new DelayCountWritable(delay, 1);
            context.write(airline, delayCount);
        } catch (NumberFormatException e) {
            // Skip invalid delay values
        }
    }
}