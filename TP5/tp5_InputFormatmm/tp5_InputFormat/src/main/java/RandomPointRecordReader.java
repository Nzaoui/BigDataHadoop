
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;



public class RandomPointRecordReader extends RecordReader<IntWritable, Point2DWritable> {

	private int Counter=0;
	private Point2DWritable p2dw = new Point2DWritable(); 
	private int Max; 
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		Max = Integer.parseInt(context.getConfiguration().get("MaxPoints"));
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (Counter < Max){
			Counter ++;
			p2dw = new Point2DWritable((Math.random()), (Math.random()));
			return true;
		}
		else
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
