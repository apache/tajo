package nta.datum;

import nta.datum.exception.InvalidCastException;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class DatumFactory {
	public static Datum createByte(byte val) {
		return new ByteDatum(val);
	}
	
	public static Datum createByte(String val) {
	  if(val.length() > 1)
	    throw new InvalidCastException("Cannot cast byte from a multi bytes");
    return new ByteDatum(val.charAt(0));
  }
	
	public static Datum createShort(short val) {
		return new ShortDatum(val);
	}
	
	public static Datum createShort(String val) {
	  return new ShortDatum(Short.valueOf(val));
	}
	
	public static Datum createInt(int val) {
		return new IntDatum(val);
	}
	
	public static Datum createInt(String val) {
	  return new IntDatum(Integer.valueOf(val));
	}
	
	public static Datum createLong(long val) {
		return new LongDatum(val);
	}
	
	public static Datum createLong(String val) {
	  return new LongDatum(Long.valueOf(val));
	}
	
	public static Datum createFloat(float val) {
		return new FloatDatum(val);
	}
	
	public static Datum createFloat(String val) {
	  return new FloatDatum(Float.valueOf(val));
	}
	
	public static Datum createDouble(double val) {
		return new DoubleDatum(val);
	}
	
	public static Datum createDouble(String val) {
	  return new DoubleDatum(Double.valueOf(val));
	}
	
	public static Datum createBool(boolean val) {
		return new BoolDatum(val);
	}
	
	public static Datum createBool(String val) {
	  boolean boolVal = val.equals("true") ? true : false;
	  return new BoolDatum(boolVal);
	}
	
	public static Datum createBytes(byte [] val) {
    return new BytesDatum(val);
  }
	
	public static Datum createBytes(String val) {
	  throw new InvalidCastException("Cannot cast bytes from a literal string");
	}
	
	public static Datum createString(String val) {
		return new StringDatum(val);
	}
	
	public static Datum createIPv4(byte [] val) {
	  return new IPv4Datum(val);
	}
	
	public static Datum createIPv4(String val) {
	  return new IPv4Datum(val);
	}
}