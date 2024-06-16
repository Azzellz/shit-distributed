package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
import java.util.Arrays;

//3. 根据投票数量（高低）对电影进行排序并输出
public class MovieSortByVote {
    public static class VoteMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable votes = new IntWritable();
        private Text movieId = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 如果这一行是标题行，直接返回，不进行处理
            if (value.toString().startsWith("id")) {
                return;
            }

            // split CSV line
            String[] fields = value.toString().split(",");

            // votes are in the fifth field
            String voteString = fields[4];
            int voteCount;
            try {
                voteCount = Integer.parseInt(voteString);
            } catch (NumberFormatException e) {
                System.err.println("Invalid vote count: " + voteString);
                return;
            }

            votes.set(voteCount);
            movieId.set(Arrays.toString(fields)); // movie id
            context.write(votes, movieId);
        }
    }
    public static class VoteReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    // 降序排序器
    public static class DescendingIntWritableComparable extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "movie vote sort");
        job.setJarByClass(MovieSortByVote.class);
        job.setMapperClass(VoteMapper.class);
        job.setReducerClass(VoteReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(DescendingIntWritableComparable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
