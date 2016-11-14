
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * TaggedValue is the "value" that we send from the Mapper to the Reducer, thus
 * it has to implement {@link Writable}.
 * 
 * It's a "structure" formed with a String that will contain the actual
 * informations, and a boolean as a "tag" to identify if the informations are
 * coming from the {@link CityMapper} or from the {@link RegionMapper}.
 * 
 * @author Naji
 *
 */
public class TaggedValue implements Writable {

	private String data;
	private boolean isACity;

	public TaggedValue() {
		super();
	}

	public TaggedValue(String d, boolean isC) {
		this.data = d;
		this.isACity = isC;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public boolean isACity() {
		return isACity;
	}

	public void setACity(boolean isACity) {
		this.isACity = isACity;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(this.isACity);
		out.writeUTF(this.data);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.isACity = in.readBoolean();
		this.data = in.readUTF();

	}

}
