/**
 * 
 */
package nta.datum;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import com.google.gson.annotations.Expose;

import nta.datum.exception.InvalidCastException;
import nta.datum.exception.InvalidOperationException;
import nta.datum.json.GsonCreator;

/**
 * @author Hyunsik Choi
 *
 */
public class BytesDatum extends Datum {
//  private int size;
	@Expose
	byte[] val;
	ByteBuffer bb = null;
	
	/**
	 * 
	 */
	public BytesDatum() {
		super(DatumType.BYTES);
	}
	
	public BytesDatum(byte [] val) {
		this();
//		this.size = val.length;
		this.val = val;
		this.bb = ByteBuffer.wrap(val);	
		bb.flip();
	}
	
	public BytesDatum(ByteBuffer val) {
		this();
//		this.size = val.limit();
		this.val = val.array();
		this.bb = val.duplicate();
		bb.flip();
	}
	
	public void initFromBytes() {
		if (bb == null) {
			bb = ByteBuffer.wrap(val);
		}
	}
	
	public boolean asBool() {
		throw new InvalidCastException("Cannot cast bytes into boolean");
	}
	
	@Override
	public short asShort() {	
		throw new InvalidCastException();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asInt()
	 */
	public int asInt() {
		initFromBytes();
		bb.rewind();
		return bb.getInt();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asLong()
	 */
	public long asLong() {
		initFromBytes();
		bb.rewind();
		return bb.getLong();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByte()
	 */
	public byte asByte() {
		initFromBytes();
		bb.rewind();
		return bb.get();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByteArray()
	 */
	public byte[] asByteArray() {
		initFromBytes();
		bb.rewind();
		return bb.array();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asFloat()
	 */
	public float asFloat() {
		initFromBytes();
		bb.rewind();
		return bb.getFloat();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asDouble()
	 */
	public double asDouble() {
		initFromBytes();
		bb.rewind();
		return bb.getDouble();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asChars()
	 */
	public String asChars() {
		initFromBytes();
		bb.rewind();
		return new String(bb.array(), Charset.defaultCharset());
	}
	
	public String toJSON() {
		return GsonCreator.getInstance().toJson(this, Datum.class);
	}

  @Override
  public int size() {
//    return size;
	  return this.val.length;
  }
  
  @Override
  public int hashCode() {
	initFromBytes();
	bb.rewind();
    return bb.hashCode();
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof BytesDatum) {
      BytesDatum other = (BytesDatum) obj;
      initFromBytes();
      other.initFromBytes();
      return bb.equals(other.bb);
    }
    
    return false;
  }

  // Datum Comparable
  public BoolDatum equalsTo(Datum datum) {
    switch (datum.type()) {
    case BYTES:
    	initFromBytes();
    	((BytesDatum)datum).initFromBytes();
      return DatumFactory.createBool(Arrays.equals(this.val, ((BytesDatum)datum).val));
    default:
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
    case BYTES:
    	initFromBytes();
    	((BytesDatum)datum).initFromBytes();
      return bb.compareTo(((BytesDatum) datum).bb);
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
}
