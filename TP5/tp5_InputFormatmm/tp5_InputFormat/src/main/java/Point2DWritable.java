import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class Point2DWritable implements Writable {

	private double x;
	private double y;
	
	public Point2DWritable(){
		
		
	}
	
	public Point2DWritable(double a, double b){
		setX(a);
		setY(b); 
	}
	public String ToString(){
		return "("+this.x+","+this.y+")";
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
