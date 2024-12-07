package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private Text genre = new Text();
    private DoubleWritable budget = new DoubleWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Assuming the input is a CSV-like string (comma-separated values)
        String line = value.toString();
        String[] columns = line.split("\t");

        if (columns.length > 23) { // Ensure there are enough columns (index 9 for genre and index 23 for budget)
            try {
                // Extract genre and budget from the appropriate columns
                String[] genres = columns[9].trim().split("\\|"); // Split genre field by "|"
                double movieBudget = Double.parseDouble(columns[23].trim()); // Budget column (index 23)

                // Loop through each genre and emit budget for each genre
                for (String g : genres) {
                    genre.set(g.trim()); // Set genre
                    budget.set(movieBudget); // Set budget
                    // Print out the genre and budget for debugging purposes
                    System.out.println("Genre: " + genre.toString() + ", Budget: " + budget.get());
                    context.write(genre, budget); // Emit genre and budget
                }
            } catch (NumberFormatException e) {
                // Handle potential parsing errors (invalid number format)
                // Skip the current line if it has invalid data
            }
        }
    }
}


