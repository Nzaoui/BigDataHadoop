
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
 * To reach our goal we made our Mappers send a Text as a key and
 * {@link TaggedValue} as a value. this TaggedValue is just a String containing
 * the name of the city/region plus a "tag" that will help us identify if the
 * data are City data or Region data.
 *
 */

public class TP7_Part1 {

	/**
	 * The CityMapper is going to read our "worldcitiespop.txt", and give us for
	 * each line a Text,TaggedValue.
	 * 
	 * Text : containing the RegionCode and CountryCode for that City but in
	 * lowerCase. Since the codes are written in lowerCases in the Region file.
	 * 
	 * TaggedValue: containing the City name, and a boolean set to True to say
	 * that it's a City.
	 */

	public static class CityMapper extends Mapper<Object, Text, Text, TaggedValue> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] Data = value.toString().split(",");

			if (!Data[4].matches("Population")) {
				String dataToSend = Data[0].concat(",").concat(Data[3]);
				context.write(new Text(dataToSend.toLowerCase()), new TaggedValue(Data[1], true));
			}

		}
	}

	/**
	 * The RegionMapper is going to read our "region_codes.csv", and give us for
	 * each line a Text,TaggedValue.
	 * 
	 * TaggedKey : containing the RegionCode and CountryCode for that Region in
	 * lowerCase, just to make sure it's going to be the same keys as the first
	 * CityMapper
	 * 
	 * TaggedValue: containing the Region name, and a boolean set to False to
	 * say that it's not a City.
	 * 
	 */
	public static class RegionMapper extends Mapper<Object, Text, Text, TaggedValue> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] Data = value.toString().split(",");
			String dataToSend = Data[0].concat(",").concat(Data[1]);
			context.write(new Text(dataToSend.toLowerCase()), new TaggedValue(Data[2], false));
		}

	}

	/**
	 * Since both our Mappers send their OutPut to the same Reducer, their
	 * OutPuts should match the InPut of the Reducer :[Text,TaggedValue]
	 * 
	 * For each set of data the Reducer receives [values], we check if it's not
	 * a city, thus a region, we save it in our variable region, and if it's a
	 * city we add it our List<> of cities, and at the end for each city in List
	 * we write the (CountryCode , RegionCode) = the Key , and then the
	 * (RegionName \t CityName) = The Value. 
	 * 
	 * At the end the Result would be like for instance  :
	 *  fr,33	Aquitaine,Bordeaux
	 *  fr,33	Aquitaine,Talence
	 *  fr,33	Aquitaine,Pessac
	 *  ...
	 */
	public static class TP7Reducer extends Reducer<Text, TaggedValue, Text, Text> {

		public void reduce(Text key, Iterable<TaggedValue> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> cities = new ArrayList<>();
			String region = "";
			for (TaggedValue value : values) {

				if (!value.isACity()) {
					region = value.getData();
				} else
					cities.add(value.getData());
			}
			for (String city : cities) {
				context.write(key, new Text(region.concat("\t").concat(city)));

			}

		}
	}

	/**
	 * In our main we had to specify that we're going to use Multiple Mappers
	 * thus the MultipleInputs.addInputPath() instead of the normal
	 * job.setInputFormat(), plus we made the necessary adjustments in the
	 * In/OutPuts
	 * 
	 */

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TP7");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP7_Part1.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CityMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RegionMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TaggedValue.class);
		job.setReducerClass(TP7Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
