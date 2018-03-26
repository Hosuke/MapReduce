package com.hosuke.mapreduce.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Hosuke
 *
A:B,C,D,F,E,O
B:A,C,E,K
C:F,A,D,I
D:A,E,F,L
E:B,C,D,M,L
F:A,B,C,D,E,O,M
G:A,C,D,E,F
H:A,C,D,E,O
I:A,O
J:B,O
K:A,C,D
L:D,E,F
M:E,F,G
O:A,H,I,J,K

给出用户与粉丝列表，求互粉好友
 */
public class Follows {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        System.setProperty("HADOOP_USER_NAME", "root");
//        conf.set("fs.defaultFS", "hdfs://localhost:9000/");
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "Follows");
        job.setJarByClass(Follows.class);

        job.setMapperClass(FollowsMapper.class);
        job.setReducerClass(FollowsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);



        String inputPath = ("/Users/Hosuke/Developer/Github/MapReduce/input/Follows.txt");
        Path outputPath = new Path("/Users/Hosuke/Developer/Github/MapReduce/output/Follows");

        FileInputFormat.setInputPaths(job, inputPath);
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        /**
         * 这两句也非常重要By Hosuke
         */
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion ? 0 : 1);

    }

    public static class FollowsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // D:A,E,F,L
            String line = value.toString();
            String[] user_fans = line.split(":");
            String user = user_fans[0];
            String[] fans = user_fans[1].split(",");
            for (String fan : fans) {
                System.out.println(fan + "_" + user);
                if (user.compareTo(fan) < 0)
                    context.write(new Text(user + "_" + fan), new IntWritable(1));
                else
                    context.write(new Text(fan + "_" + user), new IntWritable(1));
            }
        }
    }

    public static class FollowsReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            for (IntWritable v: values) {
                counter += v.get();
            }
            System.out.println(key.toString());
            if (counter > 1)
                context.write(key, NullWritable.get());
        }
    }
}
