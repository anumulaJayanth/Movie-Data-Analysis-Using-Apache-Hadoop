package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text movieTitle = new Text();
    private IntWritable numReviews = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row if present
        if (key.toString().equals("director_name,num_critic_for_reviews,duration,director_facebook_likes,actor_3_facebook_likes,actor_2_name,actor_1_facebook_likes,gross,genres,actor_1_name,movie_title,num_voted_users,cast_total_facebook_likes,actor_3_name,facenumber_in_poster,plot_keywords,movie_imdb_link,num_user_for_reviews,language,country,content_rating,budget,title_year,actor_2_facebook_likes,imdb_score,aspect_ratio,movie_facebook_likes")) {
            return;
        }

        // Assuming the input is a CSV-like string (comma-separated values)
        String line = value.toString();
        String[] columns = line.split("\t");

        if (columns.length > 21) { // Ensure we have enough columns
            try {
                String title = columns[11].trim(); // Movie title (index 10)
                int reviews = Integer.parseInt(columns[18].trim()); // Number of user reviews (index 17)
                if (!title.isEmpty() && reviews > 0) { // Emit only if title and reviews are valid
                    movieTitle.set(title);
                    numReviews.set(reviews);
                    System.out.println(title+""+reviews);
                    context.write(movieTitle, numReviews); // Emit movie title with number of reviews
                }
            } catch (Exception e) {
                // Handle errors if there's an issue parsing the data
            }
        }
    }
}
