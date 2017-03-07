package com.target.mpd.delta.mapreduce;

import com.target.mpd.delta.writable.MpdKeyWritable;
import com.target.mpd.delta.writable.MpdValueWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class TestDeltaFilterMapperReducer {
	private static MapDriver<LongWritable, Text, MpdKeyWritable, MpdValueWritable> previousMapDriver;
	private static MapDriver<LongWritable, Text, MpdKeyWritable, MpdValueWritable> currentMapDriver;
	private static MultipleInputsMapReduceDriver<MpdKeyWritable, MpdValueWritable, Text, NullWritable> mapReduceDriver;

	// Key one
	private static final Text PREVIOUS_ONE = new Text("     1     0   103  100202C     42     012-1-1");

	// Key two
	private static final Text PREVIOUS_TWO = new Text("     1     0   103  100801C     40     01-1-1");

	// Key three
	private static final Text PREVIOUS_THREE = new Text("     1     0   103  102401E     29     01-1-1");

	// Key one
	private static final Text CURRENT_ONE = new Text("     1     0   103  100202C     42     012-1-1");

	// Key Two
	private static final Text CURRENT_TWO = new Text("     1     0   103  100801C     34     01-1-1");

	// Key one
	private static final Text CURRENT_ONE_REPEAT = new Text("     1     0   103  100201C     37     01-1-1");

	private static final Text KEY_ONE_RESULT = new Text("1|0|103|1002|{(02,C,42,0,12-1-1),(01,C,37,0,1-1-1)}");
	private static final Text KEY_TWO_RESULT = new Text("1|0|103|1008|{(01,C,34,0,1-1-1)}");
	private static final Text KEY_THREE_RESULT = new Text("1|0|103|1024|{}");

	@Before
	public void setUp() {
		MpdPreviousDeltaFilterMapper mapperPrevious = new MpdPreviousDeltaFilterMapper();
		MpdCurrentDeltaFilterMapper mapperCurrent = new MpdCurrentDeltaFilterMapper();

		MpdDeltaFilterReducer reducer = new MpdDeltaFilterReducer();
		previousMapDriver = previousMapDriver.newMapDriver(mapperPrevious);
		currentMapDriver = currentMapDriver.newMapDriver(mapperCurrent);

		mapReduceDriver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(reducer);
	}

	@Test
	public void testMapReduce() throws IOException {
		MpdDeltaFilterMapper multipleInputsPrevious = new MpdPreviousDeltaFilterMapper();
		MpdDeltaFilterMapper multipleInputsCurrent = new MpdCurrentDeltaFilterMapper();

		mapReduceDriver.withMapper(multipleInputsPrevious);
		mapReduceDriver.withMapper(multipleInputsCurrent);

		mapReduceDriver.addInput(multipleInputsPrevious, new LongWritable(), PREVIOUS_ONE);
		mapReduceDriver.addInput(multipleInputsPrevious, new LongWritable(), PREVIOUS_TWO);
		mapReduceDriver.addInput(multipleInputsPrevious, new LongWritable(), PREVIOUS_THREE);

		mapReduceDriver.addInput(multipleInputsCurrent, new LongWritable(), CURRENT_ONE);
		mapReduceDriver.addInput(multipleInputsCurrent, new LongWritable(), CURRENT_TWO);
		mapReduceDriver.addInput(multipleInputsCurrent, new LongWritable(), CURRENT_ONE_REPEAT);

		mapReduceDriver.withOutput(KEY_ONE_RESULT, NullWritable.get());
		mapReduceDriver.withOutput(KEY_TWO_RESULT, NullWritable.get());
		mapReduceDriver.withOutput(KEY_THREE_RESULT, NullWritable.get());

		mapReduceDriver.runTest(false);
	}
}
