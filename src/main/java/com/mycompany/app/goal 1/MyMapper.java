package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text genre = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split("\t");
        if (columns.length > 9) {
            String[] genres = columns[9].split("\\|"); 
            for (String g : genres) {
                genre.set(g);
                context.write(genre, one);
            }
        }
    }
}


