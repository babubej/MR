package com.target.mpd.delta.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MpdKeyWritable implements WritableComparable<MpdKeyWritable> {
	private int departmentId;
	private int classId;
	private int itemId;
	private int store;

	public MpdKeyWritable() {
	}

	// To be used primarily for testing purposes.
	public MpdKeyWritable(int departmentId, int classId, int itemId, int store) {
		this.departmentId = departmentId;
		this.classId = classId;
		this.itemId = itemId;
		this.store = store;
	}

	//Parse input is provided in preference to creating a new instance every time
	public void parseInput(Text input) {
		String inputStr = input.toString();
		try {
			this.departmentId = Integer.parseInt(inputStr.substring(0, 6).trim());
			this.classId = Integer.parseInt(inputStr.substring(6, 12).trim());
			this.itemId = Integer.parseInt(inputStr.substring(12, 18).trim());
			this.store = Integer.parseInt(inputStr.substring(18, 24).trim());
		} catch (StringIndexOutOfBoundsException e) {
			throw new IllegalArgumentException("Invalid value: " + input.toString(), e);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(departmentId);
		out.writeInt(classId);
		out.writeInt(itemId);
		out.writeInt(store);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		departmentId = in.readInt();
		classId = in.readInt();
		itemId = in.readInt();
		store = in.readInt();
	}

	public int getDepartmentId() {
		return departmentId;
	}

	public int getClassId() {
		return classId;
	}

	public int getItemId() {
		return itemId;
	}

	public int getStore() {
		return store;
	}

	@Override
	public int compareTo(MpdKeyWritable other) {
		final int LESS = -1;
		final int EQUAL = 0;
		final int GREATER = 1;

		if (this == other) {
			return EQUAL;
		}

		if (this.departmentId < other.departmentId) {
			return LESS;
		}
		if (this.departmentId > other.departmentId) {
			return GREATER;
		}

		if (this.classId < other.classId) {
			return LESS;
		}
		if (this.classId > other.classId) {
			return GREATER;
		}

		if (this.itemId < other.itemId) {
			return LESS;
		}
		if (this.itemId > other.itemId) {
			return GREATER;
		}

		if (this.store < other.store) {
			return LESS;
		}
		if (this.store > other.store) {
			return GREATER;
		}

		return EQUAL;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		MpdKeyWritable that = (MpdKeyWritable) o;

		if (classId != that.classId) {
			return false;
		}
		if (departmentId != that.departmentId) {
			return false;
		}
		if (itemId != that.itemId) {
			return false;
		}
		if (store != that.store) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		int result = departmentId;
		result = 31 * result + classId;
		result = 31 * result + itemId;
		result = 31 * result + store;
		return result;
	}

	@Override
	public String toString() {
		return "MpdKeyWritable{" +
				"departmentId=" + departmentId +
				", classId=" + classId +
				", itemId=" + itemId +
				", store=" + store +
				'}';
	}
}
