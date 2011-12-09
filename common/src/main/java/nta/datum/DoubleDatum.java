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
public class DoubleDatum extends Datum {
	double val;
	
	/**
	 * 
	 */
	public DoubleDatum() {
		super(DatumType.DOUBLE);
	}
	
	public DoubleDatum(double val) {
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
		return (long) val;
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
		bb.putDouble(val);
		return bb.array();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asFloat()
	 */
	public float asFloat() {		
		return (float) val;
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
}
