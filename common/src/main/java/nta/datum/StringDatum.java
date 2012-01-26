/**
 * 
 */
package nta.datum;

import nta.datum.exception.InvalidCastException;

/**
 * @author Hyunsik Choi
 *
 */
public class StringDatum extends Datum {  
	String val;
	
	/**
	 * @param type
	 */
	public StringDatum(String val) {
		super(DatumType.STRING);
		this.val = val;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asBool()
	 */
	@Override
	public boolean asBool() {	
		throw new InvalidCastException();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByte()
	 */
	@Override
	public byte asByte() {
		throw new InvalidCastException();
	}
	
	@Override
	public short asShort() {	
		throw new InvalidCastException();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asInt()
	 */
	@Override
	public int asInt() {
		int res;
		try {
			res = Integer.valueOf(val);
		} catch (Exception e) {
			throw new InvalidCastException();
		}
		return res;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asLong()
	 */
	@Override
	public long asLong() {
		long res;
		try {
			res = Long.valueOf(val);
		} catch (Exception e) {
			throw new InvalidCastException();
		}
		return res;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByteArray()
	 */
	@Override
	public byte[] asByteArray() {		
		return val.getBytes();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asFloat()
	 */
	@Override
	public float asFloat() {
		float res;
		try {
			res = Float.valueOf(val);
		} catch (Exception e) {
			throw new InvalidCastException();
		}
		return res;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asDouble()
	 */
	@Override
	public double asDouble() {
		double res;
		try {
			res = Double.valueOf(val);
		} catch (Exception e) {
			throw new InvalidCastException();
		}
		return res;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asChars()
	 */
	@Override
	public String asChars() {
		return val;
	}

  @Override
  public int size() {
    return val.getBytes().length;
  }
}
