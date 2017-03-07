package com.target.mpd.delta.mapreduce;

import com.target.mpd.delta.writable.MpdKeyWritable;
import com.target.mpd.delta.writable.MpdValueWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

public class MpdDeltaFilter extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new MpdDeltaFilter(), args);
		System.exit(result);
	}

	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 6) {
			for (String arg : args) {
				System.out.println(arg);
			}
			throw new IllegalArgumentException("Expected 6 args: CurrentFile, PreviousFile, OutputPath, KeyTabPath, Username, Queuename.  Received " + Arrays.toString(args));
		}
		String currentPath = args[0];
		String previousPath = args[1];
		String outputPath = args[2];
		String keytab = args[3];
		String username = args[4];
		String queue = args[5];

		System.out.println("Current Path: " + currentPath);
		System.out.println("Previous Path: " + previousPath);
		System.out.println("Output Path: " + outputPath);
		System.out.println("Keytab Path: " + keytab);
		System.out.println("User Name: " + username);
		System.out.println("Queue: " + queue);

		Configuration conf = this.getConf();

		// Set Job Queue
		conf.set("mapreduce.job.queuename", queue);

		// ALL CONFIGURATION CHANGES MUST BE MADE BEFORE NOW.  CONF IS COPIED HERE.
		Job job = Job.getInstance(conf);

		conf.set("hadoop.security.authentication", "Kerberos");
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(username, keytab);

		// Output Types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapOutputKeyClass(MpdKeyWritable.class);
		job.setMapOutputValueClass(MpdValueWritable.class);

		// Job Classes
		job.setMapperClass(MpdDeltaFilterMapper.class);
		job.setReducerClass(MpdDeltaFilterReducer.class);

		job.setNumReduceTasks(20);

		MultipleInputs.addInputPath(job, new Path(currentPath), TextInputFormat.class, MpdCurrentDeltaFilterMapper.class);
		MultipleInputs.addInputPath(job, new Path(previousPath), TextInputFormat.class, MpdPreviousDeltaFilterMapper.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setJarByClass(MpdDeltaFilter.class);

		boolean result = job.waitForCompletion(true);
		return (result ? 0 : 1);
	}
}
