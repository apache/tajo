/**
 * 
 */
package nta.datum;

import nta.datum.exception.InvalidCastException;
import nta.datum.exception.InvalidOperationException;
import nta.datum.json.GsonCreator;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 *
 */
public class IPv4Datum extends Datum {
  private static final int size = 4;
  @Expose private int address;
	
	/**
	 * @param type
	 */
	public IPv4Datum() {
		super(DatumType.IPv4);
	}
	
	public IPv4Datum(String addr) {
		this();
		String [] elems = addr.split("\\.");
		address  = Integer.valueOf(elems[3]) & 0xFF;
    address |= ((Integer.valueOf(elems[2]) << 8) & 0xFF00);
    address |= ((Integer.valueOf(elems[1]) << 16) & 0xFF0000);
    address |= ((Integer.valueOf(elems[0]) << 24) & 0xFF000000);
	}
	
	public IPv4Datum(byte [] addr) {
		this();
		Preconditions.checkArgument(addr.length == size);
		address  = addr[3] & 0xFF;
    address |= ((addr[2] << 8) & 0xFF00);
    address |= ((addr[1] << 16) & 0xFF0000);
    address |= ((addr[0] << 24) & 0xFF000000);
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
		return this.address;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asLong()
	 */
	@Override
	public long asLong() {
	  return this.address;
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
	  byte[] addr = new byte[size];
	  addr[0] = (byte) ((address >>> 24) & 0xFF);
	  addr[1] = (byte) ((address >>> 16) & 0xFF);
	  addr[2] = (byte) ((address >>> 8) & 0xFF);
	  addr[3] = (byte) (address & 0xFF);
	  return addr;
	}

	@Override
	public float asFloat() {
		throw new InvalidCastException();
	}

	@Override
	public double asDouble() {
		throw new InvalidCastException();
	}

	@Override
	public String asChars() {
		return numericToTextFormat(asByteArray());
	}
	
	public String toJSON() {
		return GsonCreator.getInstance().toJson(this, Datum.class);
	}

  @Override
  public int size() {
    return size;
  }
  
  @Override
  public int hashCode() {
    return address;
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof IPv4Datum) {
      IPv4Datum other = (IPv4Datum) obj;
      return this.address == other.address;
    }
    
    return false;
  }
  
  // Datum Comparable
  public BoolDatum equalsTo(Datum datum) {
    switch (datum.type()) {
    case IPv4:    	
    	return DatumFactory.createBool(this.address == ((IPv4Datum)datum).address);
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
  
  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
    case IPv4:
      byte [] bytes = asByteArray();
      byte [] other = datum.asByteArray();
      
      for (int i = 0; i < 4; i++) {
        if (bytes[i] > other[i]) {
          return 1;
        } else if (bytes[i] < other[i]) {
          return -1;
        }
      }
      
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
  
  static String numericToTextFormat(byte[] src) {
    return (src[0] & 0xff) + "." + (src[1] & 0xff) + "." + (src[2] & 0xff)
        + "." + (src[3] & 0xff);
  }
}
