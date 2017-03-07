package com.target.mpd.delta.mapreduce;

import com.target.mpd.delta.writable.MpdKeyWritable;
import com.target.mpd.delta.writable.MpdValueWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestMpdCurrentDeltaFilterMapper {
	private static MapDriver<LongWritable, Text, MpdKeyWritable, MpdValueWritable> currentMapDriver;

	private static final MpdKeyWritable KEY_ONE = new MpdKeyWritable(1, 0, 103, 1002);
	private static final MpdKeyWritable KEY_TWO = new MpdKeyWritable(1, 0, 103, 1008);

	// Key one
	private static final Text CURRENT_ONE = new Text("     1     0   103  100202C     42     012-1-1");
	private static final MpdValueWritable VALUE_CURR_ONE = new MpdValueWritable("02", "C", 42, 0, "12-1-1", true);

	// Key Two
	private static final Text CURRENT_TWO = new Text("     1     0   103  100801C     34     01-1-1");
	private static final MpdValueWritable VALUE_CURR_TWO = new MpdValueWritable("01", "C", 34, 0, "1-1-1", true);

	// Key one
	private static final Text CURRENT_ONE_REPEAT = new Text("     1     0   103  100201C     37     01-1-1");
	private static final MpdValueWritable VALUE_CURR_ONE_REPEAT = new MpdValueWritable("01", "C", 37, 0, "1-1-1", true);

	@Before
	public void setUp() {
		MpdCurrentDeltaFilterMapper mapperCurrent = new MpdCurrentDeltaFilterMapper();
		currentMapDriver = currentMapDriver.newMapDriver(mapperCurrent);
	}

	@Test
	public void testMapper() throws IOException {
		currentMapDriver.addInput(new LongWritable(), CURRENT_ONE);
		currentMapDriver.addInput(new LongWritable(), CURRENT_TWO);
		currentMapDriver.addInput(new LongWritable(), CURRENT_ONE_REPEAT);

		currentMapDriver.withOutput(KEY_ONE, VALUE_CURR_ONE);
		currentMapDriver.withOutput(KEY_ONE, VALUE_CURR_ONE_REPEAT);
		currentMapDriver.withOutput(KEY_TWO, VALUE_CURR_TWO);

		currentMapDriver.runTest(false);
	}
}
