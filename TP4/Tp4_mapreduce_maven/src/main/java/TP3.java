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

public class TP4 {
	public static class TP4Mapper extends
			Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			/*
			 * The Mapper gets as input the file worldcitiespop.txt that
			 * contains all the cities of the world (with their Country,City
			 * Name,Accent City,Region,Population, Latitude,Longitude) and sends
			 * the result to the reducer which in our case does nothing but
			 * write the result it has been given from the Mapper.
			 * 
			 * 
			 * The Mapper work consists to: + Count all the Cities given in the
			 * file + removing the lines that doesn't contain a designated
			 * number of population (except for the first line that contains the
			 * names of the columns) + Count the cities for which we know the
			 * number of population + Count the total of all the given
			 * population
			 */

			// the counters used to calculate the number of cities given, cities
			// with an informed
			// number of population, the sum of the population respectively
			// nb_cities,nb_pop,
			// total_pop

			Counter Valid_Cities = context.getCounter("WCD", "nb_cities");
			Counter Cities_With_Pop = context.getCounter("WCD", "nb_pop");
			Counter Total_Pop = context.getCounter("WCD", "total_pop");

			// The counter nb_cities will be incremented by one each time the
			// mapper is called
			// =>for every line in the worldcitiespop.txt and that equals = for
			// every city

			Valid_Cities.increment(1);
			Text t = new Text("");

			// We split the line that the mapper is reading at every "," so that
			// we can check
			// whether or not the population of that city has been informed
			// therefore the "if"

			String Data[] = value.toString().split(",");

			if (!Data[4].isEmpty()) {

				// The goal of this test is to make the mapper ignore the first
				// line in the
				// file since it only contains the name of the columns

				if (!Data[4].matches("Population")) {

					// The entries that reach this point are all the lines,
					// starting
					// from the second one, for which we know the number of
					// population.
					// For every city with an informed number of population =>
					// increment
					// the counter nb_pop by one, and the total_pop by the
					// number of
					// population of that city.

					Cities_With_Pop.increment(1);

					Total_Pop.increment(Integer.parseInt(Data[4]));

					// ----- End of TP3-----
					
					
					/*
					 * The Goal of this exercise (TP4) is to be able to
					 * 'categorize' the given file "worldcitiespop.txt" to
					 * categories of cities and count the
					 * minimum,maximum,average of the population of each
					 * category. 
					 * ----------------- What we understood ---------------
					 * The Goal of the last exercise is to give the
					 * user the ability to 'set' the categories, it could be
					 * categories of cities like 10,100,1000 ... or it could be
					 * 4,16,64,256 ...
					 * 
					 * ----------------- What was asked for ---------------
					 * The Goal of the last exercise is to give the 
					 * user the ability to 'set' the categories, which means
					 * he could fragment the initial categories to get a more 
					 * accurate resume of all the cities in the world, the more
					 * he fragments the initial categories, the more it becomes 
					 * accurate.
					 */

					// We use the Math.Log10() and Math.pow() to get the categories 10,100,1000 ...  
					int logOfPopulation = (int) (Math.log10(Double.parseDouble(Data[4])));
					double card = (Math.pow(10,logOfPopulation));
					
					 // We retrieve the parameter given by the user, using
					 // Context.getConfiguration() that gives us the
					 // configuration used, and then we use the Get(name) to get
					 // the property that has that name, here it is "FragmentRate"
					 // that has been set when the method main was called
					
					int givenArg = Integer.parseInt((context.getConfiguration()
							.get("FragmentRate")));

					// Then we multiply the variable "card" by 10 to get the max
					// value, that is always strictly superior to the number of
					// population in that category, and we divide it by the
					// number of fragment the user wants.
					// So in the end it gives us an interval of population.
					double interval = (card*10)/givenArg;
					
					// Then we divide the number of population of each cities by
					// the interval, to see how many intervals the number of
					// population covers.
					int Temp = (int) (Integer.parseInt(Data[4]) / interval);

					// Finally we send to the Reducer a key that is the result
					// of Temp (number of intervals covered by the population of
					// that city) * interval (the product of fragmenting the
					// initial categories as many times as the user desires)
					String st = String.valueOf((int) (Temp * interval));
					Text tex = new Text(st);
					t = tex;
					context.write(t,
							new IntWritable(Integer.parseInt((Data[4]))));


					
					// //////////////////// What We Understood ...
					// 
					//
					// // We retrieve the parameter given by the user, using
					// // Context.getConfiguration() that gives us the
					// // configuration used, and then we use the Get(name) to
					// get
					// // the property that has that name, here it is
					// "BaseOfLog"
					// // that has been set when the method main was called
					// int givenArg =
					// Integer.parseInt((context.getConfiguration()
					// .get("BaseOfLog")));
					//
					// // To calculate the Logarithm in a a given base, the base
					// // will be the parameter given by the user,we've used the
					// // rule: Logb(a) = Log(a) / Log(b)
					// int logInGivenBase = (int) (Math.log(Double
					// .parseDouble(Data[4]) / Math.log(givenArg)));
					//
					//
					// String st = String.valueOf((int) (Math.pow(givenArg,
					// logInGivenBase)));
					// Text tex = new Text(st);
					// t = tex;
					//
					// // The Mapper sends to the Reducer the couple (key,value)
					// // such as the key = a category; and value = the
					// population
					// // of the Mapped city.
					// context.write(t,
					// //////////////// new
					// IntWritable(Integer.parseInt((Data[4]))));

				}
			}
		}

	}

	public static class TP4Reducer extends
			Reducer<Text, IntWritable, Text, Text> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
			/*
			 * The Reducer gets as input multiple couples (key,List<value>) sent from
			 * the Mapper, and gives as result a list of categories. Each
			 * category contains a number of cities that has been assembled with
			 * computation on their number of population.
			 * 
			 * 
			 * The Reducer work consists to: + Regroup the values (number of
			 * population) following their keys (categories) + Calculate the
			 * total of cities belonging to each category + Calculate the
			 * minimum population in each category + Calculate the Maximum of
			 * population in each category + Calculate the Average of population
			 * of each category
			 */
			
			
			
			// The variables here will be used for : 
			// avg => Average population of cities in the category ;
			// total => The number of cities belonging to the category ;
			// min,max => minimum,maximum population of city in the category ;
			// Sum => the Sum of population in that category.
			int avg = 0;
			int total = 0;
			int min = 1000000000;
			int max = 0;
			int Sum = 0;
			
			
			//For each couple (key,List<value>) given by the Mapper 
			//and for for every iteration of List<value> we will:
			//		- increment the variable total by one;
			//		- increment the variable Sum by iteration.value;
			//		- test if iteration.value is inferior to the minimum;
			//		- test if iteration.value is superior to the maximum;
			//
			//When we finish the iterations of List<value> then we compute
			//the variable avg which will be equal to Sum (containing the sum 
			//of population of cities in that category) divided by total 
			//(containing the number of cities in that category)

			for (IntWritable iteration : values) {
				total++;
				Sum += iteration.get();
				if (iteration.get() > max) {
					max = iteration.get();
				}
				if (iteration.get() < min) {
					min = iteration.get();
				}
			}
			avg = Sum / total;
			
			
			// We then put the variable one next to one (total -> avg -> min ->
			// max -> Sum) in a String that will be later be written in the
			// output file.
			String s = String.valueOf(total).concat("\t")
					.concat(String.valueOf(avg)).concat("\t")
					.concat(String.valueOf(min)).concat("\t")
					.concat(String.valueOf(max)).concat("\tthe sum is: \t")
					.concat(String.valueOf(Sum));
			context.write(key, new Text(s));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// Here we use the method set(name,value) of the Class Configuration to
		// set the parameter "FragmentRate" here to take the value given by the
		// user in the command line "args[0]" = the first argument given after
		// "yarn jar JarName.jar args[0] ..."

		conf.set("FragmentRate", args[0]);

		Job job = Job.getInstance(conf, "TP4");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP4.class);
		job.setMapperClass(TP4Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(TP4Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
