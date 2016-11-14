import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * The Sorter takes place after the {@link Grouper} which takes place after the
 * {@link Partitionner}.
 * 
 * The Sorter is going to put the data that came from the RegionMapper first in
 * the set of data that have been grouped by the Grouper, data with the same
 * "natural key".
 * 
 * To do that we need to implement the method compare() of our mother-class
 * {@link WritableComparator}, that compares two {@link WritableComparable}.
 * Since our {@link TaggedKey} is an instance of that class we can make a cast
 * and call TaggedKey.compareTo(). This method will put the data with the "tag"
 * of a region first then the data with the "tag" of a city after it, in each
 * set of data.
 * 
 * @see Partitionner
 * @see Grouper
 * 
 * @author Naji
 *
 */
public class Sorter extends WritableComparator {

	public Sorter() {
		super(TaggedKey.class, true);
	}

	@SuppressWarnings(value = { "rawtypes" })
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		return ((TaggedKey) w1).compareTo((TaggedKey) w2);
	}
}
