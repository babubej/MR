package com.target.mpd.delta.writable;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;

public class TestMpdValue {
	@Test
	public void testParse() {
		Text input = new Text("     2     7   753  227501C     39     01-3-1");
		MpdValueWritable writable = new MpdValueWritable();
		writable.parseInput(input, true);

		assertEquals("Floor not equal", "01", writable.getFloor());
		assertEquals("Block not equal", "C", writable.getBlock());
		assertEquals("Aisle not equal", 39, writable.getAisle());
		assertEquals("Section not equal", 0, writable.getSection());
		assertEquals("Display Schema not equal", "1-3-1", writable.getDisplaySchema());
		assertEquals("Filename not equal", true, writable.isCurrentFile());
	}

	@Test
	public void testWriteRead() throws IOException {
		Text input = new Text("     2     7   753  227501C     39     01-3-1");
		MpdValueWritable writable = new MpdValueWritable();
		writable.parseInput(input, true);

		ByteArrayOutputStream baos = new ByteArrayOutputStream(100);
		DataOutput dos = new DataOutputStream(baos);
		writable.write(dos);

		DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
		MpdValueWritable output = new MpdValueWritable();
		output.readFields(in);

		assertEquals("Writables are not the same", writable, output);
	}
}
