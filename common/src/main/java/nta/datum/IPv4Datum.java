/**
 * 
 */
package nta.datum;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.gson.annotations.Expose;

import nta.datum.exception.InvalidCastException;
import nta.datum.exception.InvalidOperationException;
import nta.datum.json.GsonCreator;

/**
 * @author Hyunsik Choi
 *
 */
public class IPv4Datum extends Datum {
  private static final int size = 4;
  @Expose
  byte[] val;
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
		bb.flip();
		val = bb.array();
	}
	
	public IPv4Datum(byte [] addr) {
		this();
		val = addr;
		bb = ByteBuffer.wrap(addr);
	}
	
	public void initFromBytes() {
		if (bb == null) {
			bb = ByteBuffer.wrap(val);
		}
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
		initFromBytes();
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
		initFromBytes();
		StringBuilder sb = new StringBuilder();
		bb.rewind();
		
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
	
	public String toJSON() {
		return GsonCreator.getInstance().toJson(this, Datum.class);
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
  
  @Override
  public int hashCode() {
	initFromBytes();
    return bb.hashCode();
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof IPv4Datum) {
      IPv4Datum other = (IPv4Datum) obj;
      initFromBytes();
      other.initFromBytes();
      return bb.equals(other.bb);
    }
    
    return false;
  }
  
  // Datum Comparable
  public BoolDatum equalsTo(Datum datum) {
    switch (datum.type()) {
    case IPv4:
    	initFromBytes();
    	((IPv4Datum)datum).initFromBytes();
    	return DatumFactory.createBool(Arrays.equals(this.val, ((IPv4Datum)datum).val));
      //return DatumFactory.createBool(this.bb.equals(((IPv4Datum) datum).bb));
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
  
  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
    case IPv4:
    	initFromBytes();
      return bb.compareTo(((BytesDatum)datum).bb);
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
}
