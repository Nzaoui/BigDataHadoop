
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomPointInputFormat extends InputFormat<IntWritable, Point2DWritable>  {

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {

		int NbSplits = Integer.parseInt(context.getConfiguration().get("NumberOfSplits"));
		List<InputSplit> Lis = new ArrayList<InputSplit>();
		for (int i=0; i<NbSplits;i++){
		Lis.add(new FakeInputSplit());
		}
		
		return Lis;
	}

	@Override
	public RecordReader<IntWritable, Point2DWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new RandomPointRecordReader();
	}

}
