
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * The Goal of this exercise is to implement the Join pattern in a Map-Reduce
 * program, Here we're going to use this pattern to associate the cities of the
 * world with the name of the regions they belong to, based on our file
 * "worldcitiespop.txt" that contains all the cities, and the file
 * "region_codes.cvs" that contains the regions and their countries and the
 * numbers/letters identifying that region in that country.
 * 
 * To do so, we are going to use 2 Mappers: one for the cities and the other for
 * the regions.
 * 
 * For the first part of this exercise see {@link TP7_Part1}.
 * 
 * For the second part we want to send to reducer the region first then the
 * cities, and to be able to do that we had to use our own Partitioner, Grouper
 * and a Sorter.
 * 
 * The idea here is , instead of sending a Text Key containing the Code+Country
 * of the city/region to the Reducer, We're going to send a {@link TaggedKey},
 * it's the same principle as the {@link TaggedValue} used in the first part.
 * 
 * The Partitioner is going to receive those keys and is going to send the ones
 * that have the same "Hash" to the same reducer (we're going to use two
 * reducers for that, it can also work for one), then the Grouper receives the
 * output of the partitioner and regroup the keys that are alike, finally before
 * sending it to the reducer the Sorter makes sure that the region is the first
 * to be sent in each Group of data that the Reducer is going to receive
 * 
 * @see Partitionner
 * @see Grouper
 * @see Sorter
 * 
 * @author Naji
 * 
 */
public class TP7 {

	/**
	 * The CityMapper is going to read our "worldcitiespop.txt", and give us for
	 * each line a TaggedKey,TaggedValue.
	 * 
	 * TaggedKey : containing the RegionCode and CountryCode for that City, and
	 * a "10" to say that it's a city.
	 * 
	 * TaggedValue: same as first part , containing the City name.
	 * 
	 */
	public static class CityMapper extends Mapper<Object, Text, TaggedKey, TaggedValue> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] Data = value.toString().split(",");

			if (!Data[4].matches("Population")) {
				String dataToSend = (Data[0].concat(",").concat(Data[3])).toLowerCase();
				context.write(new TaggedKey(dataToSend, 10), new TaggedValue(Data[1], true));
			}

		}
	}

	/**
	 * The RegionMapper is going to read our "region_codes.csv", and give us for
	 * each line a TaggedKey,TaggedValue.
	 * 
	 * TaggedKey : containing the RegionCode and CountryCode for that Region,
	 * and a "0" to say that it's not a City.
	 * 
	 * TaggedValue: same as first part , containing the Region name.
	 * 
	 */
	public static class RegionMapper extends Mapper<Object, Text, TaggedKey, TaggedValue> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] Data = value.toString().split(",");
			String dataToSend = (Data[0].concat(",").concat(Data[1])).toLowerCase();
			context.write(new TaggedKey(dataToSend, 0), new TaggedValue(Data[2], false));
		}

	}

	/**
	 * Since both our Mappers send their OutPut to the same Reducer, their
	 * OutPuts should match the InPut of the Reducer :[TaggedKey,TaggedValue]
	 * 
	 * We kept the same reducer as the first part, but we could have changed it:
	 * Since the region arrives first in each set of data [values], we could
	 * have taken the first element in that set and it would have been the
	 * Region and write it directly with all the elements that comes afterwards,
	 * without going through the tests.
	 */

	public static class TP7Reducer extends Reducer<TaggedKey, TaggedValue, Text, Text> {

		public void reduce(TaggedKey key, Iterable<TaggedValue> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> cities = new ArrayList<>();
			String region = "None";
			for (TaggedValue value : values) {

				if (!value.isACity()) {
					region = value.getData();
				} else
					cities.add(value.getData());
			}
			for (String city : cities) {
				context.write(new Text(key.getData()), new Text("R:" + region + "\t" + "C:" + city));
			}

		}
	}

	/**
	 * In our main we had to specify that we're going to use Multiple Mappers
	 * thus the MultipleInputs.addInputPath() instead of the normal
	 * job.setInputFormat(), plus we made the necessary adjustments in the
	 * In/OutPuts
	 * 
	 * In this part of the exercise we had to specify the Partitioner, Grouper
	 * and Sorter classes so that our job takes them in consideration:
	 * job.setPartitionerClass(), job.setGroupingComparatorClass(),
	 * job.setSortComparatorClass().
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TP7");
		job.setNumReduceTasks(2);
		job.setJarByClass(TP7.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CityMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RegionMapper.class);

		job.setMapOutputKeyClass(TaggedKey.class);
		job.setMapOutputValueClass(TaggedValue.class);

		job.setPartitionerClass(Partitionner.class);
		job.setGroupingComparatorClass(Grouper.class);
		job.setSortComparatorClass(Sorter.class);

		job.setReducerClass(TP7Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
