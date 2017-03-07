package com.target.mpd.delta.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MpdValueWritable implements Writable {
	private String floor;
	private String block;
	private int aisle;
	private int section;
	private String displaySchema;

	//Note that we dont include these in the equals().  It's being used as a flag to differentiate
	//    latest from current, not really a reflection of the overall value.
	private boolean currentFile;

	public MpdValueWritable() {
	}

	public MpdValueWritable(MpdValueWritable other) {
		this.floor = other.floor;
		this.block = other.block;
		this.aisle = other.aisle;
		this.section = other.section;
		this.displaySchema = other.displaySchema;
		this.currentFile = other.currentFile;
	}

	// To be used primarily for testing purposes.
	public MpdValueWritable(String floor, String block, int aisle, int section, String displaySchema, boolean currentFile) {
		this.floor = floor;
		this.block = block;
		this.aisle = aisle;
		this.section = section;
		this.displaySchema = displaySchema;
		this.currentFile = currentFile;
	}

	//Parse input is provided in preference to creating a new instance every time
	public void parseInput(Text input, boolean currentFile) {
		String inputStr = input.toString();
		try {
			this.floor = inputStr.substring(24, 26).trim();
			this.block = inputStr.substring(26, 28).trim();
			this.aisle = Integer.parseInt(inputStr.substring(28, 34).trim());
			this.section = Integer.parseInt(inputStr.substring(34, 40).trim());
			this.displaySchema = inputStr.substring(40).trim();
		} catch (StringIndexOutOfBoundsException e) {
			throw new IllegalArgumentException("Invalid value: " + input.toString(), e);
		}
		this.currentFile = currentFile;

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(floor);
		out.writeUTF(block);
		out.writeInt(aisle);
		out.writeInt(section);
		out.writeUTF(displaySchema);
		out.writeBoolean(currentFile);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		floor = in.readUTF();
		block = in.readUTF();
		aisle = in.readInt();
		section = in.readInt();
		displaySchema = in.readUTF();
		currentFile = in.readBoolean();
	}

	public String getFloor() {
		return floor;
	}

	public String getBlock() {
		return block;
	}

	public int getAisle() {
		return aisle;
	}

	public int getSection() {
		return section;
	}

	public String getDisplaySchema() {
		return displaySchema;
	}

	public Boolean isCurrentFile() {
		return currentFile;
	}

	//Note that we don't include filename in the equals().  It's being used as a flag to differentiate
	//    latest from current, not really a reflection of the overall value.
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		MpdValueWritable that = (MpdValueWritable) o;

		if (aisle != that.aisle) {
			return false;
		}
		if (section != that.section) {
			return false;
		}
		if (block != null ? !block.equals(that.block) : that.block != null) {
			return false;
		}
		if (displaySchema != null ? !displaySchema.equals(that.displaySchema) : that.displaySchema != null) {
			return false;
		}
		if (floor != null ? !floor.equals(that.floor) : that.floor != null) {
			return false;
		}

		return true;
	}

	//Note that we don't include filename in the equals().  It's being used as a flag to differentiate
	//    latest from current, not really a reflection of the overall value.
	@Override
	public int hashCode() {
		int result = floor != null ? floor.hashCode() : 0;
		result = 31 * result + (block != null ? block.hashCode() : 0);
		result = 31 * result + aisle;
		result = 31 * result + section;
		result = 31 * result + (displaySchema != null ? displaySchema.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "MpdValueWritable{" +
				"floor='" + floor + '\'' +
				", block='" + block + '\'' +
				", aisle=" + aisle +
				", section=" + section +
				", displaySchema='" + displaySchema + '\'' +
				", currentFile=" + currentFile +
				'}';
	}
}
