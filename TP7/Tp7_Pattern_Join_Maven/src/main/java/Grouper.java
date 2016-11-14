import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * The Grouper “assembles” values together according to the "natural key". With
 * natural key here, we mean the (CountryCode,RegionCode), the TaggedKey without
 * taking in consideration the "tag".
 * 
 * Without the Grouper we might find data with the same key treated in different
 * Reducer.reduce calls, and our final result wouldn't be correct.
 * 
 * Since partitioner breaks the data in 2 (Partition 0 : Pair / Partition 1 :
 * Impair), our Grouper needs to regroup in the first Partition the Data that
 * have the SAME key, and then do the same for the second Partition too.
 * 
 * To do that we need to implement the method compare() of our mother-class
 * {@link WritableComparator}, that compares two {@link WritableComparable}.
 * Since our {@link TaggedKey} is an instance of that class we can make a cast
 * and get the "natural key" then we compare it with the next "natural key" with
 * the String.compareTo() that returns 0 if they match and !0 if they don't. By
 * doing that we make sure that the data with same key are going to be sent to
 * the same Reducer.reduce call.
 * 
 * @see Partitionner
 * @see Sorter
 * 
 * @author Naji
 *
 */
public class Grouper extends WritableComparator {

	public Grouper() {
		super(TaggedKey.class, true);
	}

	@SuppressWarnings(value = { "rawtypes" })
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		return ((TaggedKey) w1).getData().compareTo(((TaggedKey) w2).getData());
	}

}
