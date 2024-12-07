package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, Text> {
    private Text genre = new Text();
    private Text scoreAndCount = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split("\t");

        if (columns.length > 24) { // Ensure there are enough columns
            String[] genres = columns[9].split("\\|"); // Split genres
            String imdbScore = columns[25]; // IMDB Score
            
            try {
                Double.parseDouble(imdbScore); // Validate IMDB score as a number
                for (String g : genres) {
                    genre.set(g);
                    scoreAndCount.set(imdbScore + ",1");
                    context.write(genre, scoreAndCount);
                }
            } catch (NumberFormatException e) {
                // Skip invalid or non-numeric IMDB scores
            }
        }
    }
}
