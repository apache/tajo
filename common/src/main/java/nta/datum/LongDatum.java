/**
 * 
 */
package nta.datum;

import java.nio.ByteBuffer;

import nta.datum.exception.InvalidCastException;

/**
 * @author Hyunsik Choi
 *
 */
public class LongDatum extends Datum {
  private static final int size = 8;
  
	long val;
	
	/**
	 * 
	 */
	public LongDatum() {
		super(DatumType.LONG);
	}
	
	public LongDatum(long val) {
		this();
		this.val = val;
	}
	
	public boolean asBool() {
		throw new InvalidCastException();
	}
	
	@Override
	public short asShort() {	
		return (short) val;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asInt()
	 */
	public int asInt() {		
		return (int) val;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asLong()
	 */
	public long asLong() {
		return val;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByte()
	 */
	public byte asByte() {
		throw new InvalidCastException();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByteArray()
	 */
	public byte[] asByteArray() {
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putLong(val);
		return bb.array();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asFloat()
	 */
	public float asFloat() {		
		return val;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asDouble()
	 */
	public double asDouble() {
		return val;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asChars()
	 */
	public String asChars() {
		return ""+val;
	}

  @Override
  public int size() {
    return size;
  }
}
