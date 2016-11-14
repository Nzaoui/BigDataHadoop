
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 * The class Point2DWritable is what would be used by the mapper and the reducer
 * it's the same as IntWritable that writes an int, but instead here we will be
 * writing a Point2D.
 *
 * We implement the class Writable to use the methods:
 *  - write(DataOutput) to write the instance (a Point2D).
 *  - readFields(DataInput) to read an instance (the two points of the Point2D).
 *  
 *  N.B: Take a look a look at {@link Writable} for further informations.
 *  
 */
public class Point2DWritable implements Writable {

	private double x;
	private double y;

	public Point2DWritable() {

	}

	public Point2DWritable(double a, double b) {
		setX(a);
		setY(b);
	}
	
	public String ToString() {
		return  this.x + "," + this.y ;
	}
	public Point2DWritable ToPoint(String pointString){
		String s[] = pointString.split(",");
		double x = Double.parseDouble(s[0]);
		double y = Double.parseDouble(s[1]);
		return new Point2DWritable(x, y);
	}
	public void write(DataOutput out) throws IOException {

		out.writeDouble(x);
		out.writeDouble(y);
	}

	public void readFields(DataInput in) throws IOException {

		x = in.readDouble();
		y = in.readDouble();
	}

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

}
