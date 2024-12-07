package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        int count = 0;

        for (Text val : values) {
            String[] scoreAndCount = val.toString().split(",");
            if (scoreAndCount.length == 2) {
                try {
                    double score = Double.parseDouble(scoreAndCount[0]);
                    int cnt = Integer.parseInt(scoreAndCount[1]);
                    sum += score;
                    count += cnt;
                } catch (NumberFormatException e) {
                    // Skip invalid data
                }
            }
        }

        if (count > 0) {
            double average = sum / count;
            result.set(String.format("%.2f", average)); // Format to 2 decimal places
            context.write(key, result);
        }
    }
}

