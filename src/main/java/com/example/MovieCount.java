package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//1. 统计每种电影题材的电影数量并输出
public class MovieCount {
    public static class GenreMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text genre = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 如果这一行是标题行，直接返回，不进行处理
            if (value.toString().startsWith("id,title,year")) {
                return;
            }
            // split CSV line
            String[] fields = value.toString().split(",");
            // genres are in the last field, remove brackets and split by space
            String[] genres = fields[fields.length - 1].replace("(", "").replace(")", "").split(" ");
            for (String g : genres) {
                genre.set(g);
                context.write(genre, one);
            }
        }
    }

    public static class GenreCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "movie count");
        job.setJarByClass(MovieCount.class);
        job.setMapperClass(GenreMapper.class);
        job.setCombinerClass(GenreCountReducer.class);
        job.setReducerClass(GenreCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}