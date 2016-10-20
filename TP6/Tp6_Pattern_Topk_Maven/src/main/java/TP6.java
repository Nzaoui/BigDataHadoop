
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * The Goal of this exercise is to implement the pattern TopK in a Map-Reduce
 * program, Here we're going to use this pattern to get the most populated
 * cities of the world, based on our file "worldcitiespop.txt" that contains all
 * the cities.
 */

public class TP6 {

	/**
	 * The Mapper will receive a number of lines from the main file, and his
	 * role is going to be to extract the top "K" most populated cities, from
	 * those he received.
	 * 
	 * To do so we need a "K" given by the user, and we're going to use the
	 * structure TreeMap<> to sort the cities by population, by giving the
	 * population as a key, and the cities informations as a value. Since
	 * TreeMap<> implements the interface SortedMap<>, it's going to sort the
	 * values depending on their keys. The TreeMap is going to put the value
	 * with the "bigger" key last and the one with "smallest" key will be first.
	 * The idea here is to put the city that is being read in the TreeMap and
	 * checking if the size of our TreeMap reached the "K" given by the user, if
	 * so, we remove the first value that has the smallest key = the first key.
	 * 
	 * At the end our TreeMap will contain the TopK cities read by the Mapper.
	 */
	public static class TP6Mapper extends Mapper<Object, Text, NullWritable, Text> {

		public int k = 0;
		public SortedMap<Integer, String> mapperTopKCities = new TreeMap<>();

		/**
		 * The method setup() here will be used to instantiate the variable "K"
		 * by giving it the value introduced by the user.
		 */
		@Override
		public void setup(Context context) {
			k = Integer.parseInt(context.getConfiguration().get("K"));
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] data = value.toString().split(",");
			if ((!data[4].isEmpty()) && (!data[4].matches("Population"))) {
				mapperTopKCities.put(Integer.parseInt(data[4]), data[4].concat(",").concat(data[2]));
				if (mapperTopKCities.size() > k) {
					mapperTopKCities.remove(mapperTopKCities.firstKey());
				}
			}
		}

		/**
		 * the method cleanup() is used to write our TreeMap, that contains the
		 * TopK cities from the ones that the Mapper received.
		 */
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (String cityName : mapperTopKCities.values()) {
				context.write(NullWritable.get(), new Text(cityName));
			}
		}
	}

	/**
	 * The Reducer will receive a number of Lists from the Mappers, and his role
	 * is going to be to extract the top "K" most populated cities, from those
	 * lists. Each Mapper will send his TopK cities, and the Reducer needs to
	 * sort those cities and return the TopK of all the "worldcitiespop.txt"
	 * file.
	 * 
	 * The Reducer is going to do nearly the same treatment as the Mappers: <<We
	 * need a "K" given by the user (the same as the one in the Mapper), and
	 * we're going to use the structure TreeMap<> to sort the cities by
	 * population, by giving the population as a key, and the cities
	 * informations as a value. Since TreeMap<> implements the interface
	 * SortedMap<>, it's going to sort the values depending on their keys. The
	 * TreeMap is going to put the value with the "bigger" key last and the one
	 * with "smallest" key will be first. The idea here is to put the city that
	 * is being read in the TreeMap and checking if the size of our TreeMap
	 * reached the "K" given by the user, if so, we remove the first value that
	 * has the smallest key = the first key>>
	 * 
	 * At the end our TreeMap will contain the TopK cities read by All the
	 * Mappers: TopK cities of all the file.
	 */
	public static class TP6Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

		public int k = 0;
		public SortedMap<Integer, String> reducerTopKCities = new TreeMap<>();

		@Override
		public void setup(Context context) {
			k = context.getConfiguration().getInt("K", 10);
		}

		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text cityData : values) {
				String[] data = cityData.toString().split(",");
				reducerTopKCities.put(Integer.parseInt(data[0]), cityData.toString());
				if (reducerTopKCities.size() > k) {
					reducerTopKCities.remove(reducerTopKCities.firstKey());
				}
			}
			for (String cityInfo : reducerTopKCities.values()) {
				context.write(key, new Text(cityInfo));
			}
		}
	}

	
	/**
	 * The Combiner can be considered as a Mini-Reducer, the biggest difference
	 * is that each computer in our Cluster has his own Combiner, and for the
	 * Reducer we're working with only one in the whole Cluster. The role of the
	 * Combiners is to smooth the work for the reducer. For instance: Without
	 * Combiners the Reducer is going to receive (the number of computers)*(the
	 * number of Mappers per computer) List that he needs to sort and extract
	 * the TopK cities of them, but instead with Combiners the Reducer is going
	 * to receive (the number of computers) List that needs to be sorted and
	 * extract the TopK cities of them thus it would be faster.
	 * 
	 * The Combiner is going to do exact same treatment as the Reducer.
	 */
	public static class TP6Combiner extends Reducer<NullWritable, Text, NullWritable, Text> {
		public int k = 0;
		public SortedMap<Integer, String> combinerTopKCities = new TreeMap<>();

		@Override
		public void setup(Context context) {
			k = context.getConfiguration().getInt("K", 10);
		}

		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text cityData : values) {
				String[] data = cityData.toString().split(",");
				combinerTopKCities.put(Integer.parseInt(data[0]), cityData.toString());
				if (combinerTopKCities.size() > k) {
					combinerTopKCities.remove(combinerTopKCities.firstKey());
				}
			}
			for (String cityInfo : combinerTopKCities.values()) {
				context.write(key, new Text(cityInfo));

			}

		}
	}

	/**
	 * Finally we need to specify the class of our Combiner in the main, using
	 * the method setCombinerClass() of the class Job.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("K", args[0]);
		Job job = Job.getInstance(conf, "TP6");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP6.class);
		job.setMapperClass(TP6Mapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setCombinerClass(TP6Combiner.class);

		job.setReducerClass(TP6Reducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
