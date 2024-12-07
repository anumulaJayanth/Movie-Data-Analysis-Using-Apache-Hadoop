package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        // Iterate over all the values (1's for each movie in this language) and sum them up
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum); // Set the total count for the language
        context.write(key, result); // Emit the language and its total count
    }
}
