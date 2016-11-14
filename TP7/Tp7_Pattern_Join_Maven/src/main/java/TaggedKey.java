import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * TaggedKey is the "key" that we send from the Mapper to the Reducer, thus it
 * has to implement {@link Writable}.
 * 
 * It also implements {@link Comparable}, that gives us the method compareTo()
 * called in the {@link Sorter}, this method will give us 3 result : 
 * (0 : same tag),(-10 : city with region), and (10 : region with city).
 * 
 * It's a "structure" formed with a String that will contain the actual
 * informations: natural key, and an integer as a "tag" to identify if the
 * informations are coming from the {@link CityMapper} (set to "10") or from the
 * {@link RegionMapper} (set to "0").
 * 
 * --------------------------------------------------------------------------
 * 
 * We could implement {@link Writable} only and use the compareTo() of the
 * {@link Sorter}, in it we could have done what this compareTo() is doing?
 * 
 * -------------------------------------------------------------------------- * 
 * 
 * @author Naji
 *
 */

public class TaggedKey implements WritableComparable<TaggedKey> {

	private String data;
	private int isACity;

	public TaggedKey(String d, int isAC) {
		super();
		this.data = d;
		this.isACity = isAC;
	}

	public TaggedKey() {
		super();
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public int isACity() {
		return this.isACity;
	}

	public void setACity(int isACity) {
		this.isACity = isACity;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.isACity);
		out.writeUTF(this.data);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		this.isACity = in.readInt();
		this.data = in.readUTF();
	}

	/**
	 * 
	 * compareTo(AnotherTaggedKey) compare the two, by comparing their natural
	 * key first and then their "tags".
	 * 
	 * -If the tag is the same result is going to be (0), in other words if
	 * those two keys came from the same mapper the result of this method is
	 * zero.
	 * 
	 * -If the first one came from CityMapper and the second from RegionMapper
	 * the result is going to be (-10) that would put the data with the first
	 * TaggedKey last in the set of data.
	 * 
	 * -If the first one came from RegionMapper and the second from CityMapper
	 * the result is going to be (10) and that would put the data with the first
	 * Taggedkey first in the set of data.
	 *
	 **/
	@Override
	public int compareTo(TaggedKey o) {
		Integer tag1 = this.isACity();
		Integer tag2 = o.isACity();
		int result = this.getData().compareTo(o.getData());
		int differentTag = tag1.compareTo(tag2);
		if (result == 0) {
			result = -1 * differentTag;
		}
		return result;
	}

}
