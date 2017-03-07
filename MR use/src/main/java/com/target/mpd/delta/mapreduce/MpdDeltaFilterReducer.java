package com.target.mpd.delta.mapreduce;

import com.target.mpd.delta.writable.MpdKeyWritable;
import com.target.mpd.delta.writable.MpdValueWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class MpdDeltaFilterReducer extends Reducer<MpdKeyWritable, MpdValueWritable, Text, NullWritable> {
	private static final Text textWritable = new Text();
	private static final NullWritable nullWritable = NullWritable.get();
	protected static final String FIELD_SEP = "|";
	protected static final String SET_OPEN = "{";
	protected static final String SET_CLOSE = "}";
	protected static final String VALUE_SEP = ",";
	protected static final String VALUE_OPEN = "(";
	protected static final String VALUE_CLOSE = ")";

	@Override
	public void reduce(MpdKeyWritable key, Iterable<MpdValueWritable> values, Context context) throws IOException, InterruptedException {
		//Discard the first value
		Iterator<MpdValueWritable> iter = values.iterator();

		MpdValueWritable value;
		Set<MpdValueWritable> previousSet = new HashSet<MpdValueWritable>();
		Set<MpdValueWritable> currentSet = new HashSet<MpdValueWritable>();
		while (iter.hasNext()) {
			//Copy to avoid the reused instance
			value = new MpdValueWritable(iter.next());
			if (value.isCurrentFile()) {
				currentSet.add(value);
			} else {
				previousSet.add(value);
			}
		}

		if (!previousSet.equals(currentSet)) {
			//   values differ for both (either in value or existence). Output current file's set, even if empty.
			//   These cases end up being equivalent in output.
			context.write(buildOutputText(key, currentSet), nullWritable);
		}

	}

	protected Text buildOutputText(MpdKeyWritable key, Set<MpdValueWritable> values) {
		StringBuilder output = new StringBuilder();
		output.append(key.getDepartmentId()).append(FIELD_SEP);
		output.append(key.getClassId()).append(FIELD_SEP);
		output.append(key.getItemId()).append(FIELD_SEP);
		output.append(key.getStore()).append(FIELD_SEP);
		output.append(SET_OPEN);

		Iterator<MpdValueWritable> valueIter = values.iterator();
		while (valueIter.hasNext()) {
			MpdValueWritable value = valueIter.next();
			output.append(VALUE_OPEN);
			output.append(value.getFloor());
			output.append(VALUE_SEP);
			output.append(value.getBlock());
			output.append(VALUE_SEP);
			output.append(value.getAisle());
			output.append(VALUE_SEP);
			output.append(value.getSection());
			output.append(VALUE_SEP);
			output.append(value.getDisplaySchema());
			output.append(VALUE_CLOSE);

			if (valueIter.hasNext()) {
				output.append(VALUE_SEP);
			}
		}

		output.append(SET_CLOSE);

		textWritable.set(output.toString());

		return textWritable;
	}
}
