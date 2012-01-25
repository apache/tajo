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
public class ByteDatum extends Datum {
	byte val;
	
	/**
	 * 
	 */
	public ByteDatum() {
		super(DatumType.BYTE);
	}
	
	public ByteDatum(byte val) {
		this();
		this.val = val;
	}
	
	public ByteDatum(char val) {
	  this();
	  this.val = (byte) val;
	}
	
	public boolean asBool() {
		throw new InvalidCastException();
	}
	
	@Override
	public short asShort() {	
		throw new InvalidCastException();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asInt()
	 */
	public int asInt() {		
		return val;
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
		return val;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByteArray()
	 */
	public byte[] asByteArray() {
		ByteBuffer bb = ByteBuffer.allocate(1);
		bb.put(val);
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
		return "0x"+val;
	}
}
