package com.hosuke.mapreduce.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

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

    以上是数据：
    A:B,C,D,F,E,O
    表示：B,C,D,E,F,O是A用户的好友。

    求所有两两用户之间的共同好友
 */
public class MutualFriends {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
//        System.setProperty("HADOOP_USER_NAME", "root");
//        conf.set("fs.defaultFS", "hdfs://localhost:9000/");
        FileSystem fs = FileSystem.get(conf);

        Job job1 = Job.getInstance(conf, "MutualFriend1");

        job1.setMapperClass(MF1Mapper.class);
        job1.setReducerClass(MF1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);


        String inputPath = "/Users/Hosuke/Developer/Github/MapReduce/input/MF.txt";
//        String inputPath = args[0];
        String outputPath1 = "/Users/Hosuke/Developer/Github/MapReduce/output/MF1semi";
//        String outputPath = args[1];

        FileInputFormat.setInputPaths(job1, inputPath);
        if(fs.exists(new Path(outputPath1))){
            fs.delete(new Path(outputPath1), true);
        }
        FileOutputFormat.setOutputPath(job1, new Path(outputPath1));


        Job job2 = Job.getInstance(conf, "MutualFriend2");
        job2.setMapperClass(MF2Mapper.class);
        job2.setReducerClass(MF2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        String outputPath2 = "/Users/Hosuke/Developer/Github/MapReduce/output/MF1";

        FileInputFormat.setInputPaths(job2, outputPath1);
        if(fs.exists(new Path(outputPath2))){
            fs.delete(new Path(outputPath2), true);
        }
        FileOutputFormat.setOutputPath(job2, new Path(outputPath2));

        /**
         * 多job串联这种需求中，每个job的各种组件，该写还得写，
         *
         * JobControl这个类只是把具有依赖关系的多个job串联起来。然后调度执行了。
         */


        JobControl control = new JobControl("CF");

        ControlledJob ajob = new ControlledJob(job1.getConfiguration());
        ControlledJob bjob = new ControlledJob(job2.getConfiguration());

        bjob.addDependingJob(ajob);

        control.addJob(ajob);
        control.addJob(bjob);


        Thread t = new Thread(control);
        t.start();

        while(!control.allFinished()){
            Thread.sleep(1000);
        }

        System.exit(0);

    }

    public static class MF1Mapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // F:A,B,C,D,E,O,M
            String line = value.toString();
            String[] person_friends = line.split(":");
            String person = person_friends[0];
            String[] friends = person_friends[1].split(",");
            for (String friend : friends)
                context.write(new Text(friend), new Text(person));
        }
    }

    public static class MF1Reducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer persons = new StringBuffer();
            int counter = 0;
            for (Text t : values) {
                t.toString();
                counter ++;
                if (counter > 1)
                    persons.append(",");
                persons.append(t.toString());
            }
            context.write(key, new Text(persons.toString()));
        }
    }

    public static class MF2Mapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // A	F,I,O,K,G,D,C,H,B
            String line = value.toString();
            String[] friend_persons = line.split("\t");
            String friend = friend_persons[0];
            String[] persons = friend_persons[1].split(",");
            Arrays.sort(persons);

            for (int i = 0; i < persons.length - 1; i++) {
                for (int j = i+1; j < persons.length; j++) {
                    context.write(new Text(persons[i] + "-" + persons[j]), new Text(friend));
                }
            }
        }
    }

    public static class MF2Reducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer friends = new StringBuffer();
            int counter = 0;
            for (Text t : values) {
                t.toString();
                counter ++;
                if (counter > 1)
                    friends.append(",");
                friends.append(t.toString());
            }
            context.write(key, new Text(friends.toString()));
        }
    }
}
