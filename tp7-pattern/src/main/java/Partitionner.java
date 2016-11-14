import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 
 * The Partitioner is called directly after the Mapper and before the Reducer,
 * the partitioner is the one controlling the partitioning of the outputs of the
 * Mapper/Mappers based on the Keys of the data.
 * 
 * In Other words, the partitioner take the OutPuts of the Mappers and break it
 * to partitions of data following the Key and how we implement the method
 * getPatitioner(), often it's implemented as a Hash() method.
 * 
 * Here in our exercise we take the String in our key (Since our key is a
 * {@link TaggedKey}) and we apply on it the String.hashCode(), which return the
 * Hash of our String and we do a modulo with the numPartitions, that represents
 * none than the number of Reduce tasks in our job.
 * 
 * For instance, since the number of our Reduce tasks is 2 (indicated in the
 * main of {@link TP7}), the return of our getPartition() is going to be either
 * 1 or 0. the Partition 0 is going to contain all the data, for which the hash
 * of its key.getData() is pair. Same for Partition 1 : Impair hash of
 * key.getData().
 * 
 * @see Grouper
 * @see Sorter
 *
 * @author Naji
 *
 */
public class Partitionner extends Partitioner<TaggedKey, TaggedValue> {

	@Override
	public int getPartition(TaggedKey key, TaggedValue value, int numPartitions) {

		return key.getData().hashCode() % numPartitions;

	}

}
