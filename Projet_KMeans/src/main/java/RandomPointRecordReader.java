
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * The class RandomPointRecordReader is a class extending the abstract class
 * Record<key,value>, the RecordReadon is the one responsible for breaking the
 * data to the mapper.
 * 
 * Here we've used the IntWritable as a key, and a Point2DWritable as the value.
 * 
 * N.B: Take a look a look at {@link RecordReader} for further informations.
 **/


public class RandomPointRecordReader extends RecordReader<IntWritable, Point2DWritable> {

	/**
	 * We use the variables: 
	 * 
	 * - Counter : to keep a count of how many points this object have created, 
	 * 				and will also be used as the key to each
	 * 				Point2DWritable. 
	 * - p2dw : a Point2DWritable a point that will be return each time the
	 *  		method getCurrentValue() will be called. 
	 * - Max : it's a value that will be given by the user,to tell us how
	 *   		many points should be created by each Mapper.
	 * 
	 */
	private int Counter = 0;
	private Point2DWritable p2dw = new Point2DWritable();
	private int Max;

	/*
	 * As seen in the Javadoc of the RecordReader, this method will be called
	 * once at initialization.
	 * 
	 * We've used it to retrieve the Number of points that should be created by
	 * each mapper : Max, using the context given in parameter we use the
	 * methods getConfiguration() then get("Variable_Name").
	 */

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		Max = Integer.parseInt(context.getConfiguration().get("MaxPoints"));

	}

	/*
	 * In our method nextKeyValue we check if we reached the Max number of points
	 * wanted, if not ( Counter < Max ) we increment our Counter and create a new
	 * Point2DWritable, and return true. once we reach the Max, we return false.
	 *  
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (Counter < Max) {
			Counter++;
			p2dw = new Point2DWritable((Math.random()), (Math.random()));
			return true;
		} else
			return false;
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return new IntWritable(Counter);
	}

	@Override
	public Point2DWritable getCurrentValue() throws IOException, InterruptedException {
		return p2dw;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {

	}

}
