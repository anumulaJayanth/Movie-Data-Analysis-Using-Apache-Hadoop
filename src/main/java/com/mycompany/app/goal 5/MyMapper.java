
package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text language = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row if present
        if (key.toString().equals("director_name,num_critic_for_reviews,duration,director_facebook_likes,actor_3_facebook_likes,actor_2_name,actor_1_facebook_likes,gross,genres,actor_1_name,movie_title,num_voted_users,cast_total_facebook_likes,actor_3_name,facenumber_in_poster,plot_keywords,movie_imdb_link,num_user_for_reviews,language,country,content_rating,budget,title_year,actor_2_facebook_likes,imdb_score,aspect_ratio,movie_facebook_likes")) {
            return;
        }

        // Assuming the input is a CSV-like string (comma-separated values)
        String line = value.toString();
        String[] columns = line.split("\t");

        if (columns.length > 21) { // Check if there are enough columns (index 21 for language)
            try {
                String lang = columns[17].trim(); // Language column (index 21)
                if (!lang.isEmpty()) { // Ensure the language is not empty
                    language.set(lang);

                    

                    context.write(language, one); // Emit the language with count 1
                    System.out.println("Emitting language: " + lang); 
                }
            } catch (Exception e) {
                // Handle potential errors (e.g., missing language field)
                // Skip the current line if the language field is invalid
            }
        }
    }
}


