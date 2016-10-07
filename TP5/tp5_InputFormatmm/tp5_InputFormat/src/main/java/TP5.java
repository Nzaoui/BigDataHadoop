import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP5 {
	public static class TP5Mapper extends
			Mapper<Object, RandomPointInputFormat, IntWritable, Point2DWritable> {

		public void map(Object key, RandomPointInputFormat value, Context context)
				throws IOException, InterruptedException {
			
			for(InputSplit iS :	value.getSplits(context))
			{
				boolean b = true;
				RecordReader<IntWritable,Point2DWritable> rR = value.createRecordReader(iS, context);

				rR.initialize(iS, context);
				while(b)
				{
					
					b = rR.nextKeyValue();
					
					context.write(rR.getCurrentKey(), rR.getCurrentValue());
					
				
				}
			}			
			}

	}

	public static class TP5Reducer extends
			Reducer<IntWritable, Point2DWritable, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Point2DWritable> values,
				Context context) throws IOException, InterruptedException {
			
			for(Point2DWritable value : values )
			context.write(key,new Text(value.ToString()));

			
		
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

	
		conf.set("MaxPoints", args[0]);
		conf.set("NumberOfSplits", args[1]);

		Job job = Job.getInstance(conf, "TP5");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP5.class);
		job.setMapperClass(TP5Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(TP5Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	//	job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(RandomPointInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
