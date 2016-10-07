import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;


public class FakeInputSplit extends InputSplit implements Writable {
	String S[]={};

	public void write(DataOutput out) throws IOException {
		
	}

	public void readFields(DataInput in) throws IOException {
		
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		
		return 4;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return S;
	}

}
