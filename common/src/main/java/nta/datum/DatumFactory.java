package nta.datum;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class DatumFactory {
	public static Datum create(byte val) {
		return new ByteDatum(val);
	}
	
	public static Datum create(short val) {
		return new ShortDatum(val);
	}
	
	public static Datum create(int val) {
		return new IntDatum(val);
	}
	
	public static Datum create(long val) {
		return new LongDatum(val);
	}
	
	public static Datum create(float val) {
		return new FloatDatum(val);
	}
	
	public static Datum create(double val) {
		return new DoubleDatum(val);
	}
	
	public static Datum create(boolean val) {
		return new BoolDatum(val);
	}
	
	public static Datum create(String val) {
		return new StringDatum(val);
	}
}
