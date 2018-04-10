package com.hosuke.mapreduce.kmeans;

import java.io.*;
import java.net.URI;
import java.util.Vector;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

/**
 * @author Hosuke
 *
 * 运行前请先修改输入输出路径
 */
public class KMeans extends Configured implements Tool {
    private static final int MAXITERATIONS = 6;
    private static final double THRESHOLD = 10;
    private static boolean StopSignalFromReducer = false;
    private static int NoChangeCount = 0;

    public static class Point implements Writable {
        public static final int DIMENTION = 2;
        public double arr[];

        public Point() {
            arr = new double[DIMENTION];
            for (int i = 0; i < DIMENTION; ++i) {
                arr[i] = 0;
            }
        }

        public static double getEulerDist(Point vec1, Point vec2) {
            if (!(vec1.arr.length == DIMENTION && vec2.arr.length == DIMENTION)) {
                System.exit(1);
            }
            double dist = 0.0;
            for (int i = 0; i < DIMENTION; ++i) {
                dist += (vec1.arr[i] - vec2.arr[i]) * (vec1.arr[i] - vec2.arr[i]);
            }
            return Math.sqrt(dist);
        }

        public static double getManhtDist(Point vec1, Point vec2) {
            if (!(vec1.arr.length == DIMENTION && vec2.arr.length == DIMENTION)) {
                System.exit(1);
            }
            double dist = 0.0;
            for (int i = 0; i < DIMENTION; ++i) {
                dist += Math.abs(vec1.arr[i] - vec2.arr[i]);
            }
            return dist;
        }

        public void clear() {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = 0.0;
            }
        }

        public String toString() {
            DecimalFormat df = new DecimalFormat("0.0000");
            String rect = String.valueOf(df.format(arr[0]));
            for (int i = 1; i < DIMENTION; i++) {
                rect += "," + String.valueOf(df.format(arr[i]));
            }
            return rect;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            String str[] = in.readUTF().split(",");
            for (int i = 0; i < DIMENTION; ++i) {
                arr[i] = Double.parseDouble(str[i]);
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.toString());
        }
    }

    public static boolean stopIteration(Configuration conf) throws IOException //called in main
    {
        System.out.println("Stopping Iteration...");
        FileSystem fs = FileSystem.get(conf);

        // TODO:请更改输入输出目录
        Path pervCenterFile = new Path("/Users/Hosuke/Developer/Github/MapReduce/input/kmeans/initK");
        Path currentCenterFile = new Path("/Users/Hosuke/Developer/Github/MapReduce/output/newCentroid/part-r-00000");
        if (!(fs.exists(pervCenterFile) && fs.exists(currentCenterFile))) {
            System.out.println("Stopping error");
            System.exit(1);
        }
        //check whether the centers have changed or not to determine to do iteration or not
        boolean stop = true;
        String line1, line2;
        FSDataInputStream in1 = fs.open(pervCenterFile);
        FSDataInputStream in2 = fs.open(currentCenterFile);
        InputStreamReader isr1 = new InputStreamReader(in1);
        InputStreamReader isr2 = new InputStreamReader(in2);
        BufferedReader br1 = new BufferedReader(isr1);
        BufferedReader br2 = new BufferedReader(isr2);
        Point prevCenter, currCenter;
        while ((line1 = br1.readLine()) != null && (line2 = br2.readLine()) != null) {
            prevCenter = new Point();
            currCenter = new Point();
            String[] str1 = line1.split(",");
            String[] str2 = line2.split(",");
            for (int i = 0; i < Point.DIMENTION; i++) {
                prevCenter.arr[i] = Double.parseDouble(str1[i]);
                currCenter.arr[i] = Double.parseDouble(str2[i]);
            }
            if (Point.getEulerDist(prevCenter, currCenter) > THRESHOLD) {
                stop = false;
                break;
            }
        }
        //if another iteration is needed, then replace previous centroids with current centroids
        if (stop == false) {
            fs.delete(pervCenterFile, true);
            if (fs.rename(currentCenterFile, pervCenterFile) == false) {
                System.out.println("renaming error");
                System.exit(1);
            }
        }
        return stop;
    }

    public static class ClusterMapper extends Mapper<LongWritable, Text, Text, Text>  //output<centroid,point>
    {
        Vector<Point> centers = new Vector<Point>();
        Point point = new Point();
        int k = 0;

        @Override
        //clear centers
        public void setup(Context context) {
            try {
//                Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
//                if (caches == null || caches.length <= 0) {
//                    System.out.println("setup error");
//                    System.exit(1);
//                }
//                BufferedReader br = new BufferedReader(new FileReader(caches[0].toString()));

                // TODO:请更改输入输出目录
                FileReader fr = new FileReader(new File("/Users/Hosuke/Developer/Github/MapReduce/input/kmeans/initK"));

                BufferedReader br = new BufferedReader(fr);
                Point point;
                String line;
                while ((line = br.readLine()) != null) {
                    point = new Point();
                    String[] str = line.split(",");
                    for (int i = 0; i < Point.DIMENTION; i++) {
                        point.arr[i] = Double.parseDouble(str[i]);
                    }
                    centers.add(point);
                    k++;
                }
            } catch (Exception e) {
            }
        }

        @Override
        //output<centroid,point>
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            int index = -1;
            double minDist = Double.MAX_VALUE;
            String[] str = value.toString().split(",");
            for (int i = 0; i < Point.DIMENTION; i++) {
                point.arr[i] = Double.parseDouble(str[i]);
            }

            for (int i = 0; i < k; i++) {
                double dist = Point.getEulerDist(point, centers.get(i));
                if (dist < minDist) {
                    minDist = dist;
                    index = i;
                }
            }
            context.write(new Text(centers.get(index).toString()), new Text(point.toString()));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

        }
    }

    public static class Combiner extends Reducer<Text, Text, Text, Text> //value=Point_Sum+count
    {
        @Override
        //update every centroid except the last one
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Point sumPoint = new Point();
            String outputValue;
            int count = 0;
            while (values.iterator().hasNext()) {
                String line = values.iterator().next().toString();
                String[] str1 = line.split(":");

                if (str1.length == 2) {
                    count += Integer.parseInt(str1[1]);
                }

                String[] str = str1[0].split(",");
                for (int i = 0; i < Point.DIMENTION; i++) {
                    sumPoint.arr[i] += Double.parseDouble(str[i]);
                }
                count++;
            }
            outputValue = sumPoint.toString() + ":" + String.valueOf(count);
            context.write(key, new Text(outputValue));
        }
    }

    public static class UpdateCenterReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void setup(Context context) {

        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            Point sumPoint = new Point();
            Point newCenterPoint = new Point();
            String outputKey;
            while (values.iterator().hasNext()) {
                String line = values.iterator().next().toString();
                String[] str = line.split(":");
                String[] pointStr = str[0].split(",");
                count += Integer.parseInt(str[1]);
                for (int i = 0; i < Point.DIMENTION; i++) {
                    sumPoint.arr[i] += Double.parseDouble(pointStr[i]);
                }
            }
            for (int i = 0; i < Point.DIMENTION; i++) {
                newCenterPoint.arr[i] = sumPoint.arr[i] / count;
            }
            String[] str = key.toString().split(",");
            if (newCenterPoint.arr[0] - Double.parseDouble(str[0]) <= THRESHOLD && newCenterPoint.arr[1] - Double.parseDouble(str[1]) <= THRESHOLD) // compare old and new centroids
            {
                NoChangeCount++;
            }
            if (NoChangeCount == 10) {
                StopSignalFromReducer = true;
            }
            context.write(new Text(newCenterPoint.toString()), new Text("," + String.valueOf(NoChangeCount)));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Job job = new Job(conf);
        job.setJarByClass(KMeans.class);

        // TODO:请更改输入输出目录
        FileInputFormat.setInputPaths(job, new Path("/Users/Hosuke/Developer/Github/MapReduce/input/kmeans/kmeans"));
        Path outDir = new Path("/Users/Hosuke/Developer/Github/MapReduce/output/newCentroid");
        fs.delete(outDir, true);
        FileOutputFormat.setOutputPath(job, outDir);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(ClusterMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(UpdateCenterReducer.class);
        job.setNumReduceTasks(1);//
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // TODO:请更改输入输出目录
        Path dataFile = new Path("/Users/Hosuke/Developer/Github/MapReduce/input/kmeans/initK");
        URI uriUser = new URI("file:/Users/Hosuke/Developer/Github/MapReduce/input/kmeans/initK");

//        DistributedCache.addCacheFile(dataFile.toUri(), conf);


        int iteration = 1;
        int success = 1;
        do {
            success ^= ToolRunner.run(conf, new KMeans(), args);
            iteration++;
        } while (success == 1 && iteration < MAXITERATIONS && (!stopIteration(conf)) && !StopSignalFromReducer);


        // for final output(just a mapper only task)
        Job job = Job.getInstance(conf, "KMeans");
        // Add cache
        job.addCacheFile(uriUser);
        job.setJarByClass(KMeans.class);

        // TODO:请更改输入输出目录
        FileInputFormat.setInputPaths(job, "/Users/Hosuke/Developer/Github/MapReduce/input/kmeans/kmeans");
        Path outDir = new Path("/Users/Hosuke/Developer/Github/MapReduce/output/kmeans");
        fs.delete(outDir, true);
        FileOutputFormat.setOutputPath(job, outDir);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(ClusterMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Point.class);
        job.setOutputValueClass(Point.class);

        job.waitForCompletion(true);

    }
}