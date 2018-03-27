package com.hosuke.mapreduce.topn.userLocation;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author Hosuke
 * 
 * 描述：

		对同一个用户，在同一个位置，连续的多条记录进行合并
		合并原则：开始时间取最早的，停留时长加和
		
		字段：
		userID, locationID, time, duration
		
		数据样例：
		user_a,location_a,2018-01-01 08:00:00,60
		
		逻辑处理：
		1、按照userid和locationid分组
		2、按照userid和locationid和time排序



测试数据：
user_a	location_a	2018-01-01 08:00:00	60
user_a	location_a	2018-01-01 09:00:00	60
user_a	location_a	2018-01-01 11:00:00	60
user_a	location_a	2018-01-01 12:00:00	60
user_a	location_b	2018-01-01 10:00:00	60
user_a	location_c	2018-01-01 08:00:00	60
user_a	location_c	2018-01-01 09:00:00	60
user_a	location_c	2018-01-01 10:00:00	60
user_b	location_a	2018-01-01 15:00:00	60
user_b	location_a	2018-01-01 16:00:00	60
user_b	location_a	2018-01-01 18:00:00	60


结果数据：
user_a	location_a	2018-01-01 08:00:00	120
user_a	location_a	2018-01-01 11:00:00	120
user_a	location_b	2018-01-01 10:00:00	60
user_a	location_c	2018-01-01 08:00:00	180
user_b	location_a	2018-01-01 15:00:00	120
user_b	location_a	2018-01-01 18:00:00	60


 */
public class UserLocationMR {

	public static void main(String[] args) throws Exception {
		// 指定hdfs相关的参数
		Configuration conf = new Configuration();
		//		conf.set("fs.defaultFS", "hdfs://hadoop02:9000");
		//		System.setProperty("HADOOP_USER_NAME", "hadoop");

		Job job = Job.getInstance(conf);
		// 设置jar包所在路径
		job.setJarByClass(UserLocationMR.class);

		// 指定mapper类和reducer类
		job.setMapperClass(UserLocationMRMapper.class);
		job.setReducerClass(UserLocationMRReducer.class);

		// 指定maptask的输出类型
		job.setMapOutputKeyClass(UserLocation.class);
		job.setMapOutputValueClass(NullWritable.class);
		// 指定reducetask的输出类型
		job.setOutputKeyClass(UserLocation.class);
		job.setOutputValueClass(NullWritable.class);

		job.setGroupingComparatorClass(UserLocationGC.class);

		// 指定该mapreduce程序数据的输入和输出路径
		Path inputPath = new Path("/Users/Hosuke/Developer/Github/MapReduce/input/userLocation");
		Path outputPath = new Path("/Users/Hosuke/Developer/Github/MapReduce/output/userLocation");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		// 最后提交任务
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion ? 0 : 1);
	}

	private static class UserLocationMRMapper extends Mapper<LongWritable, Text, UserLocation, NullWritable> {

		UserLocation outKey = new UserLocation();

		/**
		 * value = user_a,location_a,2018-01-01 12:00:00,60
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] split = value.toString().split(",");

			outKey.set(split);

			context.write(outKey, NullWritable.get());
		}
	}

	private static class UserLocationMRReducer extends Reducer<UserLocation, NullWritable, UserLocation, NullWritable> {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		UserLocation outKey = new UserLocation();

		/**
		 * user_a	location_a	2018-01-01 08:00:00	60
		 * user_a	location_a	2018-01-01 09:00:00	60
		 * user_a	location_a	2018-01-01 11:00:00	60
		 * user_a	location_a	2018-01-01 12:00:00	60
		 */
		@Override
		protected void reduce(UserLocation key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

			int count = 0;
			for (NullWritable nvl : values) {
				count++;
				// 如果是这一组key-value中的第一个元素时，直接赋值给outKey对象。基础对象
				if (count == 1) {
					// 复制值
					outKey.set(key);
				} else {

					// 有可能连续，有可能不连续，  连续则继续变量， 否则输出
					long current_timestamp = 0;
					long last_timestamp = 0;
					try {
						// 这是新遍历出来的记录的时间戳
						current_timestamp = sdf.parse(key.getTime()).getTime();
						// 这是上一条记录的时间戳 和 停留时间之和
						last_timestamp = sdf.parse(outKey.getTime()).getTime() + outKey.getDuration() * 60 * 1000;
					} catch (ParseException e) {
						e.printStackTrace();
					}

					// 如果相等，证明是连续记录，所以合并
					if (current_timestamp == last_timestamp) {
						/**
						 * 更新outKey的Duration时长
						 */
						outKey.setDuration(outKey.getDuration() + key.getDuration());

					} else {

						// 先输出上一条记录
						context.write(outKey, nvl);

						// 然后再次记录当前遍历到的这一条记录
						outKey.set(key);
					}
				}
			}
			// 最后无论如何，还得输出最后一次
			context.write(outKey, NullWritable.get());
		}
	}
}
