package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable totalMovies = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get(); // Sum the movie count for each actor
        }

        // Only emit actors who have appeared in more than 10 movies
        if (sum > 10) {
            totalMovies.set(sum);
            context.write(key, totalMovies); // Emit actor name and total movie count
        }
    }
}
