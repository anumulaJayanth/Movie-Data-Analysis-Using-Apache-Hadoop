package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text actorName = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line into columns (assuming tab-delimited)
        String[] columns = value.toString().split("\t");
        
        // Check if columns for actor names are present (actor_1_name, actor_2_name, actor_3_name)
        if (columns.length > 10) { 
            String[] actors = {columns[10]}; // actor_1_name, actor_2_name, actor_3_name
            for (String actor : actors) {
                if (!actor.trim().isEmpty()) { // Skip empty names
                    actorName.set(actor.trim());
                    context.write(actorName, one); // Emit actor with count 1
                }
            }
        }
    }
}
