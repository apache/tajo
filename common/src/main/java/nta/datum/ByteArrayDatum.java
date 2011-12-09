/**
 * 
 */
package nta.datum;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import nta.datum.exception.InvalidCastException;

/**
 * @author Hyunsik Choi
 *
 */
public class ByteArrayDatum extends Datum {
	ByteBuffer bb = null;
	
	/**
	 * 
	 */
	public ByteArrayDatum() {
		super(DatumType.BYTE);
	}
	
	public ByteArrayDatum(byte [] val) {
		this();
		bb = ByteBuffer.wrap(val);
	}
	
	public ByteArrayDatum(ByteBuffer val) {
		this();
		bb = val.duplicate();
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
		bb.flip();
		return bb.getInt();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asLong()
	 */
	public long asLong() {
		bb.flip();
		return bb.getLong();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByte()
	 */
	public byte asByte() {
		bb.flip();
		return bb.get();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByteArray()
	 */
	public byte[] asByteArray() {
		return bb.array();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asFloat()
	 */
	public float asFloat() {
		bb.flip();
		return bb.getFloat();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asDouble()
	 */
	public double asDouble() {
		bb.flip();
		return bb.getDouble();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asChars()
	 */
	public String asChars() {
		return new String(bb.array(), Charset.defaultCharset());
	}
}
