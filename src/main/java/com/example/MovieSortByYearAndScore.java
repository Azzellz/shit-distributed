package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

//2. 根据电影题材分类电影，并按年份（大小），分数（高低）输出
public class MovieSortByYearAndScore {

    public static class MovieMapper extends Mapper<LongWritable, Text, Movie, IntWritable> {
        private Movie movie = new Movie();
        private IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 如果这一行是标题行，直接返回，不进行处理
            if (value.toString().startsWith("id,title,year")) {
                return;
            }
            // split CSV line
            String[] fields = value.toString().split(",");
            // genres are in the last field
            String genres = fields[fields.length - 1].replace("(", "").replace(")", "");
            String title = fields[1];
            // 解析年份
            String yearString = fields[2];
            int year;
            if(yearString.contains("/")) {
                // 如果年份字段包含 "/", 则只保留 "/" 前的部分
                yearString = yearString.split("/")[0];
            }
            try {
                year = Integer.parseInt(yearString);
            } catch (NumberFormatException e) {
                // 如果转换失败，打印错误信息并忽略这一行数据
                System.err.println("Invalid year: " + yearString);
                return;
            }
            float rating = Float.parseFloat(fields[3]);

            movie.set(genres, title, year, rating);
            context.write(movie, one);
        }
    }

    public static class MovieReducer extends Reducer<Movie, IntWritable, Movie, IntWritable> {
        public void reduce(Movie key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "movie sort");
        job.setJarByClass(MovieSortByYearAndScore.class);
        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Movie.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}