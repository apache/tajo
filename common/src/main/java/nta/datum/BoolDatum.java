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
public class BoolDatum extends Datum {
	final boolean val;
	/**
	 * @param type
	 */
	public BoolDatum(boolean val) {
		super(DatumType.BOOLEAN);
		this.val = val;
	}
	
	public boolean asBool() {
		return val;
	}
	
	@Override
	public short asShort() {	
		return (short) (val == true ? 1 : 0);
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asInt()
	 */
	@Override
	public int asInt() {
		return val == true ? 1 : 0;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asLong()
	 */
	@Override
	public long asLong() {
		return val == true ? 1 : 0;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByte()
	 */
	@Override
	public byte asByte() {
		return (byte) (val == true ? 0x01 : 0x00);
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByteArray()
	 */
	@Override
	public byte[] asByteArray() {
	  ByteBuffer bb = ByteBuffer.allocate(1);
	  bb.put(asByte());
	  return bb.array();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asFloat()
	 */
	@Override
	public float asFloat() {
		return val == true ? 1 : 0;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asDouble()
	 */
	@Override
	public double asDouble() {
		return val == true ? 1 : 0;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asChars()
	 */
	@Override
	public String asChars() {
		return val == true ? "true" : "false";
	}

  @Override
  public int size() {
    return 1;
  }
  
  @Override
  public int hashCode() {
    return val == true ? 1 : 0;
  }
}
