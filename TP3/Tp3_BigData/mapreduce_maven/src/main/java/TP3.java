import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP3 {
  public static class TP3Mapper
       extends Mapper<Object, Text, Text, IntWritable>{
	  
	public void map(Object key, Text value, Context context
			  ) throws IOException, InterruptedException {
		 
		/*
		   * The Mapper gets as input the file worldcitiespop.txt that contains all the 
		   * cities of the world (with their Country,City Name,Accent City,Region,Population,
		   * Latitude,Longitude) and sends the result to the reducer which in our case does 
		   * nothing but write the result it has been given from the mapper.
		   * 
		   * 
		   * The Mapper does all the work: 
		   * 	- Count all the Cities given in the file
		   * 	- removing the lines that doesn't contain a designated number of population
		   * 	(except for the first line that contains the names of the columns)
		   * 	- Count the cities for which we know the number of population
		   * 	- Count the total of all the given population
		*/
		
		
		
		
		//the counter used to calculate the number of cities given, cities with an informed
		 //number of population, the sum of the population respectively nb_cities,nb_pop,
		 //total_pop
		
		  Counter Valid_Cities = context.getCounter("WCD", "nb_cities");
		  Counter Cities_With_Pop = context.getCounter("WCD", "nb_pop");
		  Counter Total_Pop = context.getCounter("WCD", "total_pop");
		  
		 
		  
		  
		//The counter nb_cities will be incremented by one each time the mapper is called 
		 // =>for every line in the worldcitiespop.txt and that equals = for every city
		  
		  Valid_Cities.increment(1);
		  
		  
		  
		//We split the line that the mapper is reading at every "," so that we can check
		 //whether or not the population of that city has been informed therefore the "if"
		  
		  String Data[]=value.toString().split(",");
		  if (!Data[4].isEmpty())
		  {
			  
			  
			  //The goal of this test is to make the mapper ignore the first line in the 
			   //file since it only contains the name of the columns
			  
			  if(!Data[4].matches("Population"))
			  {
				  
				  
				  //The entries that reach this point are all the lines, starting 
				   //from the second one, for which we know the number of population.
				  //For every city with an informed number of population => increment
				   //the counter nb_pop by one, and the total_pop by the number of 
				   //population of that city.
				  
				  Cities_With_Pop.increment(1);
				  Total_Pop.increment(Integer.parseInt(Data[4]));
			  }
			  context.write(value, new IntWritable(1));
		  }
	  }
  }
  public static class TP3Reducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      context.write(key, null);
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "TP3");
    job.setNumReduceTasks(1);
    job.setJarByClass(TP3.class);
    job.setMapperClass(TP3Mapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(TP3Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
