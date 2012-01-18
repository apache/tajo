package nta.datum;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class DatumFactory {
	public static Datum createByte(byte val) {
		return new ByteDatum(val);
	}
	
	public static Datum createShort(short val) {
		return new ShortDatum(val);
	}
	
	public static Datum createInt(int val) {
		return new IntDatum(val);
	}
	
	public static Datum createLong(long val) {
		return new LongDatum(val);
	}
	
	public static Datum createFloat(float val) {
		return new FloatDatum(val);
	}
	
	public static Datum createDouble(double val) {
		return new DoubleDatum(val);
	}
	
	public static Datum createBool(boolean val) {
		return new BoolDatum(val);
	}
	
	public static Datum createBytes(byte [] val) {
    return new BytesDatum(val);
  }
	
	public static Datum createString(String val) {
		return new StringDatum(val);
	}
	
	public static Datum createIPv4(byte [] val) {
	  return new IPv4Datum(val);
	}
}