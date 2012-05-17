/**
 * 
 */
package nta.datum;

import java.nio.ByteBuffer;

import nta.datum.exception.InvalidOperationException;
import nta.datum.json.GsonCreator;

import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 */
public class BoolDatum extends Datum {
	@Expose private boolean val;

	public BoolDatum(boolean val) {
		super(DatumType.BOOLEAN);
		this.val = val;
	}
	
	public boolean asBool() {
		return val;
	}

  public void setValue(boolean val) {
    this.val = val;
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
	
	public String toJSON() {
		return GsonCreator.getInstance().toJson(this, Datum.class);
	}

  @Override
  public int size() {
    return 1;
  }
  
  @Override
  public int hashCode() {
    return val == true ? 1 : 0;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BoolDatum) {
      BoolDatum other = (BoolDatum) obj;
      return val == other.val;
    }
    
    return false;
  }
  
  // Datum Comparator
  public BoolDatum equalsTo(Datum datum) {
    switch(datum.type()) {
      case BOOLEAN: return DatumFactory.createBool(this.val == 
          ((BoolDatum)datum).val);
      default:
        throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
    case BOOLEAN:
      if (val == true && datum.asBool() == false) {
        return -1;
      } else if (val == false && datum.asBool() == true) {
        return 1;
      } else {
        return 0;
      }
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
}
