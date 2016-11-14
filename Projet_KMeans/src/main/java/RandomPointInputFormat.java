
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * The RandomPointInputFormat extends the InputFormat class that describes the
 * input-specification for a Map-Reduce job. Our class will split the input
 * using the method getSplits(), and generate a {@link RandomPointRecordReader}
 * using the method createRecordReader().
 * 
 * - getSplits() will return a List of {@link FakeInputSplit}, the number of
 * those {@link FakeInputSplit}s will be given by the user in the command line.
 * 
 * - createRecordReader() will return a new {@link RandomPointRecordReader}
 * doing so, will call the initialize() of the {@link RandomPointRecordReader}
 * that will set the Max number of Points per {@link FakeInputSplit} in the List
 * and will call nextKeyValue() as long as it returns True, Thus creating our
 * Max number of points.
 * 
 */
public class RandomPointInputFormat extends InputFormat<IntWritable, Point2DWritable> {

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {

		int NbSplits = Integer.parseInt(context.getConfiguration().get("NumberOfSplits"));
		List<InputSplit> Lis = new ArrayList<InputSplit>();
		for (int i = 0; i < NbSplits; i++) {
			Lis.add(new FakeInputSplit());
		}

		return Lis;
	}

	@Override
	public RecordReader<IntWritable, Point2DWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new RandomPointRecordReader();
	}

}
