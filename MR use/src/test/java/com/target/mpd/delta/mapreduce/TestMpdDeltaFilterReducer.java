package com.target.mpd.delta.mapreduce;

import com.target.mpd.delta.writable.MpdKeyWritable;
import com.target.mpd.delta.writable.MpdValueWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class TestMpdDeltaFilterReducer {
	private static ReduceDriver<MpdKeyWritable, MpdValueWritable, Text, NullWritable> reduceDriver;

	// Key one
	private static final MpdKeyWritable KEY_ONE = new MpdKeyWritable(1, 0, 103, 1002);
	private static final MpdValueWritable VALUE_PREV_ONE = new MpdValueWritable("02", "C", 42, 0, "12-1-1", false);

	// Key two
	private static final MpdKeyWritable KEY_TWO = new MpdKeyWritable(1, 0, 103, 1008);
	private static final MpdValueWritable VALUE_PREV_TWO = new MpdValueWritable("01", "C", 40, 0, "1-1-1", false);

	// Key three
	private static final MpdKeyWritable KEY_THREE = new MpdKeyWritable(1, 0, 103, 1024);
	private static final MpdValueWritable VALUE_PREV_THREE = new MpdValueWritable("01", "E", 29, 0, "1-1-1", false);

	// Key one
	private static final MpdValueWritable VALUE_CURR_ONE = new MpdValueWritable("02", "C", 42, 0, "12-1-1", true);

	// Key Two
	private static final MpdValueWritable VALUE_CURR_TWO = new MpdValueWritable("01", "C", 34, 0, "1-1-1", true);

	// Key one
	private static final MpdValueWritable VALUE_CURR_ONE_REPEAT = new MpdValueWritable("01", "C", 37, 0, "1-1-1", true);

	private static final Text KEY_ONE_RESULT = new Text("1|0|103|1002|{(02,C,42,0,12-1-1),(01,C,37,0,1-1-1)}");
	private static final Text KEY_TWO_RESULT = new Text("1|0|103|1008|{(01,C,34,0,1-1-1)}");
	private static final Text KEY_THREE_RESULT = new Text("1|0|103|1024|{}");

	@Before
	public void setUp() {
		MpdDeltaFilterReducer reducer = new MpdDeltaFilterReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testReducerSame() throws IOException {
		ArrayList<MpdValueWritable> values = new ArrayList<MpdValueWritable>();
		values.add(VALUE_PREV_ONE);
		values.add(VALUE_CURR_ONE);
		reduceDriver.withInput(KEY_ONE, values);
		reduceDriver.runTest(false);
	}

	@Test
	public void testReducerSingleKeyDifferent() throws IOException {
		ArrayList<MpdValueWritable> values = new ArrayList<MpdValueWritable>();
		values.add(VALUE_PREV_TWO);
		values.add(VALUE_CURR_TWO);
		reduceDriver.withInput(KEY_TWO, values);
		reduceDriver.withOutput(KEY_TWO_RESULT, NullWritable.get());
		reduceDriver.runTest(false);
	}

	@Test
	public void testReducerMultipleKeyDifferent() throws IOException {
		ArrayList<MpdValueWritable> keyValuesOne = new ArrayList<MpdValueWritable>();
		keyValuesOne.add(VALUE_PREV_ONE);
		keyValuesOne.add(VALUE_CURR_ONE);
		keyValuesOne.add(VALUE_CURR_ONE_REPEAT);

		ArrayList<MpdValueWritable> keyValuesTwo = new ArrayList<MpdValueWritable>();
		keyValuesTwo.add(VALUE_PREV_TWO);
		keyValuesTwo.add(VALUE_CURR_TWO);

		ArrayList<MpdValueWritable> keyValuesThree = new ArrayList<MpdValueWritable>();
		keyValuesThree.add(VALUE_PREV_THREE);

		reduceDriver.withInput(KEY_ONE, keyValuesOne);
		reduceDriver.withInput(KEY_TWO, keyValuesTwo);
		reduceDriver.withInput(KEY_THREE, keyValuesThree);

		reduceDriver.withOutput(KEY_ONE_RESULT, NullWritable.get());
		reduceDriver.withOutput(KEY_TWO_RESULT, NullWritable.get());
		reduceDriver.withOutput(KEY_THREE_RESULT, NullWritable.get());

		reduceDriver.runTest(false);
	}
}
