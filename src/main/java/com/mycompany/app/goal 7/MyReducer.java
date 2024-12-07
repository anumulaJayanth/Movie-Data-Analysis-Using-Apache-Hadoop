package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable highestReviews = new IntWritable();
    private Text highestMovieTitle = new Text();
    private static final int MIN_REVIEWS = 2000; // Minimum reviews threshold (updated to 500)

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalReviews = 0;

        // Calculate total reviews for the current movie (key)
        for (IntWritable val : values) {
            totalReviews += val.get();
        }

        // Keep track of the movie with the highest number of reviews (greater than 500)
        if (totalReviews > MIN_REVIEWS) {
            // Check if this movie has the highest review count encountered so far
            if (totalReviews > highestReviews.get()) {
                highestReviews.set(totalReviews);
                highestMovieTitle.set(key); // Set the movie with the highest reviews

                System.out.println(key+""+totalReviews);
            }



            // Output movies with more than 500 reviews to both file and command line
            context.write(key, new IntWritable(totalReviews));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Output the movie with the highest number of reviews greater than 500
        if (highestReviews.get() > MIN_REVIEWS) {
            context.write(highestMovieTitle, highestReviews);
        }
    }
}
