package com.target.mpd.delta.mapreduce;

import com.target.mpd.delta.writable.MpdKeyWritable;
import com.target.mpd.delta.writable.MpdValueWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class MpdDeltaFilterMapper extends Mapper<LongWritable, Text, MpdKeyWritable, MpdValueWritable> {
	protected boolean current;
	private final MpdKeyWritable keyWritable = new MpdKeyWritable();
	private final MpdValueWritable valueWritable = new MpdValueWritable();

	MpdDeltaFilterMapper(boolean current) {
		this.current = current;
	}

	@Override
	public final void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
		keyWritable.parseInput(value);
		valueWritable.parseInput(value, current);
		context.write(keyWritable, valueWritable);
	}
}
