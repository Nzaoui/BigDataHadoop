
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;



/**
 * The Class FakeInputSplit is a class extending the abstract class InputSplit
 * which represents the data to be processed by an individual Mapper.
 * 
 * It also implements the interface Writable, the class that implements it have
 * a simple, efficient, serialization protocol, based on DataInput and
 * DataOutput, but in our case we will leave its methods empty.
 * 
 *  N.B: Take a look a look at {@link InputSplit} for further informations}.
 * 
 */

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
