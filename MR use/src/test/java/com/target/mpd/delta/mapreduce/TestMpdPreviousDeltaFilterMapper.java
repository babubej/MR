package com.target.mpd.delta.mapreduce;

import com.target.mpd.delta.writable.MpdKeyWritable;
import com.target.mpd.delta.writable.MpdValueWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestMpdPreviousDeltaFilterMapper {
	private static MapDriver<LongWritable, Text, MpdKeyWritable, MpdValueWritable> previousMapDriver;

	// Key one
	private static final Text PREVIOUS_ONE = new Text("     1     0   103  100202C     42     012-1-1");
	private static final MpdKeyWritable KEY_ONE = new MpdKeyWritable(1, 0, 103, 1002);
	private static final MpdValueWritable VALUE_PREV_ONE = new MpdValueWritable("02", "C", 42, 0, "12-1-1", false);

	// Key two
	private static final Text PREVIOUS_TWO = new Text("     1     0   103  100801C     40     01-1-1");
	private static final MpdKeyWritable KEY_TWO = new MpdKeyWritable(1, 0, 103, 1008);
	private static final MpdValueWritable VALUE_PREV_TWO = new MpdValueWritable("01", "C", 40, 0, "1-1-1", false);

	// Key three
	private static final Text PREVIOUS_THREE = new Text("     1     0   103  102401E     29     01-1-1");
	private static final MpdKeyWritable KEY_THREE = new MpdKeyWritable(1, 0, 103, 1024);
	private static final MpdValueWritable VALUE_PREV_THREE = new MpdValueWritable("01", "E", 29, 0, "1-1-1", false);

	@Before
	public void setUp() {
		MpdPreviousDeltaFilterMapper mapperPrevious = new MpdPreviousDeltaFilterMapper();
		previousMapDriver = previousMapDriver.newMapDriver(mapperPrevious);
	}

	@Test
	public void testMapper() throws IOException {
		previousMapDriver.addInput(new LongWritable(), PREVIOUS_ONE);
		previousMapDriver.addInput(new LongWritable(), PREVIOUS_TWO);
		previousMapDriver.addInput(new LongWritable(), PREVIOUS_THREE);

		previousMapDriver.withOutput(KEY_ONE, VALUE_PREV_ONE);
		previousMapDriver.withOutput(KEY_TWO, VALUE_PREV_TWO);
		previousMapDriver.withOutput(KEY_THREE, VALUE_PREV_THREE);

		previousMapDriver.runTest(false);
	}
}
