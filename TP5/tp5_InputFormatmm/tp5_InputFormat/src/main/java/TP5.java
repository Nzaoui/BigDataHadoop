import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP5 {
	public static class TP5Mapper extends
			Mapper<IntWritable, Point2DWritable, IntWritable, Point2DWritable> {

		public void map(IntWritable key, Point2DWritable value, Context context)
				throws IOException, InterruptedException {

			context.write(key, value);

		}
	}

	public static class TP5Reducer extends
			Reducer<IntWritable, Point2DWritable, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Point2DWritable> values,
				Context context) throws IOException, InterruptedException {
			
			Counter Number_Of_Points = context.getCounter("Count", "NumberOfPoints");
			for (Point2DWritable value : values){
				
				Number_Of_Points.increment(1);
				context.write(key, new Text(value.ToString()));
			}
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
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Point2DWritable.class);
		job.setReducerClass(TP5Reducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(RandomPointInputFormat.class);
		// FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
