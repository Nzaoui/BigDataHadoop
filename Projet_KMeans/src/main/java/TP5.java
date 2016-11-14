import java.io.IOException;
import java.util.ArrayList;
import java.util.List;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class TP5 {
	public static class TP5Mapper extends Mapper<IntWritable, Point2DWritable, Point2DWritable, Point2DWritable> {


		public void map(IntWritable key, Point2DWritable value, Context context)
				throws IOException, InterruptedException {
			List<Point2DWritable> pointsk = new ArrayList<Point2DWritable>();
			double MinDistance = 100000; 
			int	NearestK = 0;
			for(int i=0;i<Integer.parseInt(context.getConfiguration().get("k"));i++){
				Point2DWritable pointk = new Point2DWritable().ToPoint(context.getConfiguration().get("point"+i));
				pointsk.add(pointk);
			}
			for(Point2DWritable ptsk: pointsk){
				double tempMin = Math.sqrt((Math.pow((ptsk.getX()-value.getX()), 2))+(Math.pow((ptsk.getY()-value.getY()), 2)));
				if(tempMin<MinDistance){
					MinDistance = tempMin;
					NearestK = pointsk.indexOf(ptsk);
				}
			}
			
			context.write(pointsk.get(NearestK), value);

		}
	}


	public static class TP5Reducer extends Reducer<IntWritable, Point2DWritable, IntWritable, Text> {


		public void reduce(IntWritable key, Iterable<Point2DWritable> values, Context context)
				throws IOException, InterruptedException {

//			Counter Valid_Points = context.getCounter("count", "ValidPoints");
//			Counter Number_Of_Points = context.getCounter("Count", "NumberOfPoints");
//
//			for (Point2DWritable value : values) {
//
//				Number_Of_Points.increment(1);
//
//				if (((Math.pow(value.getX(), 2)) + (Math.pow(value.getY(), 2))) < 1) {
//					Valid_Points.increment(1);
//				}
//			}
//			double ptsV = Valid_Points.getValue();
//			double nbrpts = Number_Of_Points.getValue();
//			double result = (ptsV / nbrpts) * 4;
//			System.out.println(result);
//			String str = "We have generated : " + nbrpts
//					+ " Points and that gives us an approximative value of Pi that is :" + result;
//			context.write(key, new Text(str));

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		conf.set("MaxPoints", args[0]);
		conf.set("NumberOfSplits", args[1]);
		
		conf.set("k", args[2]);
		for(int i=0;i<Integer.parseInt(conf.get("k"));i++){
			Point2DWritable pointk = new Point2DWritable((Math.random()), (Math.random()));
			conf.set("point"+i,pointk.ToString());
		}
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
		job.setInputFormatClass(RandomPointInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
