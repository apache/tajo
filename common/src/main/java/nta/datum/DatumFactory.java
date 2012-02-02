package nta.datum;

import nta.datum.exception.InvalidCastException;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class DatumFactory {
  public static BoolDatum createBool(String val) {
    boolean boolVal = val.equals("true") ? true : false;
    return new BoolDatum(boolVal);
  }
  
  public static BoolDatum createBool(boolean val) {
    return new BoolDatum(val);
  }
  
	public static ByteDatum createByte(byte val) {
		return new ByteDatum(val);
	}
	
	public static ByteDatum createByte(String val) {
	  if(val.length() > 1)
	    throw new InvalidCastException("Cannot cast byte from a multi bytes");
    return new ByteDatum(val.charAt(0));
  }
	
	public static ShortDatum createShort(short val) {
		return new ShortDatum(val);
	}
	
	public static ShortDatum createShort(String val) {
	  return new ShortDatum(Short.valueOf(val));
	}
	
	public static IntDatum createInt(int val) {
		return new IntDatum(val);
	}
	
	public static IntDatum createInt(String val) {
	  return new IntDatum(Integer.valueOf(val));
	}
	
	public static LongDatum createLong(long val) {
		return new LongDatum(val);
	}
	
	public static LongDatum createLong(String val) {
	  return new LongDatum(Long.valueOf(val));
	}
	
	public static FloatDatum createFloat(float val) {
		return new FloatDatum(val);
	}
	
	public static FloatDatum createFloat(String val) {
	  return new FloatDatum(Float.valueOf(val));
	}
	
	public static Datum createDouble(double val) {
		return new DoubleDatum(val);
	}
	
	public static Datum createDouble(String val) {
	  return new DoubleDatum(Double.valueOf(val));
	}
	
  public static StringDatum createString(String val) {
    return new StringDatum(val);
  }
	
	public static BytesDatum createBytes(byte [] val) {
    return new BytesDatum(val);
  }
	
	public static BytesDatum createBytes(String val) {
	  return new BytesDatum(val.getBytes());
	}
	
	public static IPv4Datum createIPv4(byte [] val) {
	  return new IPv4Datum(val);
	}
	
	public static IPv4Datum createIPv4(String val) {
	  return new IPv4Datum(val);
	}
}