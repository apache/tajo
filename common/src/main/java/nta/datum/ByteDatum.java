/**
 * 
 */
package nta.datum;

import com.google.gson.annotations.Expose;

import nta.datum.exception.InvalidOperationException;
import nta.datum.json.GsonCreator;

/**
 * @author Hyunsik Choi
 */
public class ByteDatum extends Datum {
  private static final int size = 1;
  
  @Expose	byte val;
	
	public ByteDatum() {
		super(DatumType.BYTE);
	}
	
	public ByteDatum(byte val) {
		this();
		this.val = val;
	}

  public ByteDatum(byte [] bytes) {
    this(bytes[0]);
  }
	
	public ByteDatum(char val) {
	  this();
	  this.val = (byte) val;
	}

  @Override
  public char asChar() {
    return (char)val;
  }

	@Override
	public int asInt() {		
		return val;
	}

  @Override
	public long asLong() {
		return val;
	}

  @Override
	public byte asByte() {
		return val;
	}

  @Override
	public byte[] asByteArray() {
    byte [] bytes = new byte[1];
    bytes[0] = this.val;
		return bytes;
	}

  @Override
	public float asFloat() {		
		return val;
	}

  @Override
	public double asDouble() {
		return val;
	}

  @Override
	public String asChars() {
		return "0x"+val;
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
    return val;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ByteDatum) {
      ByteDatum other = (ByteDatum) obj;
      return val == other.val;
    }
    
    return false;
  }

  @Override
  public BoolDatum equalsTo(Datum datum) {
    switch (datum.type()) {
    case BYTE:
      return DatumFactory.createBool(this.val == (((ByteDatum) datum).val));
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
  
  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
    case BYTE:
      if (val < datum.asByte() ) {
        return -1;
      } else if (val > datum.asByte()) {
        return 1;
      } else {
        return 0;
      }
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
}
