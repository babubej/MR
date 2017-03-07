package com.target.mpd.delta.writable;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMpdWritable {
	@Test
	public void testParse() {
		Text input = new Text("     2     7   753  227501C     39     01-3-1");
		MpdKeyWritable writable = new MpdKeyWritable();
		writable.parseInput(input);

		assertEquals("Department Id not equal", 2, writable.getDepartmentId());
		assertEquals("Class Id not equal", 7, writable.getClassId());
		assertEquals("Item Id not equal", 753, writable.getItemId());
		assertEquals("Store not equal", 2275, writable.getStore());
	}

	@Test
	public void testWriteRead() throws IOException {
		Text input = new Text("     2     7   753  227501C     39     01-3-1");
		MpdKeyWritable writable = new MpdKeyWritable();
		writable.parseInput(input);

		ByteArrayOutputStream baos = new ByteArrayOutputStream(100);
		DataOutput dos = new DataOutputStream(baos);
		writable.write(dos);

		DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
		MpdKeyWritable output = new MpdKeyWritable();
		output.readFields(in);

		assertEquals("Writables are not the same", writable, output);
	}

	@Test
	public void testCompareToDeptId() {
		MpdKeyWritable writableOne = new MpdKeyWritable(0, 20, 20, 20);
		MpdKeyWritable writableTwo = new MpdKeyWritable(5, 20, 20, 20);

		assertTrue(writableOne.compareTo(writableTwo) < 0);
		assertTrue(writableTwo.compareTo(writableOne) > 0);
	}

	@Test
	public void testCompareToClassId() {
		MpdKeyWritable writableOne = new MpdKeyWritable(0, 15, 20, 20);
		MpdKeyWritable writableTwo = new MpdKeyWritable(0, 20, 20, 20);

		assertTrue(writableOne.compareTo(writableTwo) < 0);
		assertTrue(writableTwo.compareTo(writableOne) > 0);
	}

	@Test
	public void testCompareToItemId() {
		MpdKeyWritable writableOne = new MpdKeyWritable(0, 20, 15, 20);
		MpdKeyWritable writableTwo = new MpdKeyWritable(0, 20, 20, 20);

		assertTrue(writableOne.compareTo(writableTwo) < 0);
		assertTrue(writableTwo.compareTo(writableOne) > 0);
	}

	@Test
	public void testCompareToStore() {
		MpdKeyWritable writableOne = new MpdKeyWritable(0, 20, 20, 15);
		MpdKeyWritable writableTwo = new MpdKeyWritable(0, 20, 20, 20);

		assertTrue(writableOne.compareTo(writableTwo) < 0);
		assertTrue(writableTwo.compareTo(writableOne) > 0);
	}
}
