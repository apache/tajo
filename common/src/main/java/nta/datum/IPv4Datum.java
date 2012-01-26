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
public class IPv4Datum extends Datum {
  private static final int size = 4;
  
	ByteBuffer bb;
	
	/**
	 * @param type
	 */
	public IPv4Datum() {
		super(DatumType.IPv4);
	}
	
	public IPv4Datum(String addr) {
		this();
		bb = ByteBuffer.allocate(4);
		String [] elems = addr.split("\\.");
		
		byte c;
		for(int i=0; i < 4; i++) {
			c = (byte) (0xFF & Integer.valueOf(elems[i]).byteValue());
			bb.put(c);
		}
	}
	
	public IPv4Datum(byte [] addr) {
		this();
		bb = ByteBuffer.wrap(addr);
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
	@Override
	public int asInt() {
		throw new InvalidCastException();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asLong()
	 */
	@Override
	public long asLong() {
		throw new InvalidCastException();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByte()
	 */
	@Override
	public byte asByte() {
		throw new InvalidCastException();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByteArray()
	 */
	@Override
	public byte[] asByteArray() {
		return bb.array();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asFloat()
	 */
	@Override
	public float asFloat() {
		throw new InvalidCastException();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asDouble()
	 */
	@Override
	public double asDouble() {
		throw new InvalidCastException();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asChars()
	 */
	@Override
	public String asChars() {
		StringBuilder sb = new StringBuilder();
		bb.flip();

//		short elem = (short)bb.get();
//		sb.append(elem).append(".");		
//		elem = (short)bb.get();
//		sb.append(elem).append(".");
//		elem = (short)bb.get();
//		sb.append(elem).append(".");
//		elem = (short)bb.get();
//		sb.append(elem);
		
		Byte b = bb.get();
		
		sb.append(toInt(b)).append(".");
		b = bb.get();
		sb.append(toInt(b)).append(".");
		b = bb.get();
		sb.append(toInt(b)).append(".");
		b = bb.get();
		sb.append(toInt(b));
		
		return sb.toString();
	}
	
	private int toInt(Byte b) {
		int n = b.intValue();
		if (n < 0) {
			n += 256;
		}
		return n;
	}

  @Override
  public int size() {
    return size;
  }
}
