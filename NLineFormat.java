package com.dwh.session;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NLineFormat {
	public static class WordMap extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws InterruptedException, IOException {
			System.out.println("----key----"+key);
			System.out.println("----value----"+value);
			StringTokenizer tokenizer = new StringTokenizer(value.toString(),
					" ");
			while (tokenizer.hasMoreTokens()) {
				context.write(new Text(tokenizer.nextToken()), one);
			}
		}
	}
	public static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			System.out.println("---key----"+key+"-----result-----"+result);
			context.write(key,result);
		}
	}



	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.lineinputformat.linespermap", "2");		
		Job job = new Job(conf, "word count");
		job.setJarByClass(KeyValueFormat.class);
		job.setNumReduceTasks(2);
		job.setMapperClass(WordMap.class);
		job.setReducerClass(WordReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(NLineInputFormat.class);
		FileInputFormat.addInputPaths(job,"/home/susheel/Desktop/DWH_Session/nline");
		FileOutputFormat.setOutputPath(job, new Path("/home/susheel/Desktop/DWH_Session/outputnline"));
		job.waitForCompletion(true);

	}
}
