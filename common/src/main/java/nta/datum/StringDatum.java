/**
 * 
 */
package nta.datum;

import com.google.gson.annotations.Expose;

import nta.datum.exception.InvalidCastException;
import nta.datum.exception.InvalidOperationException;
import nta.datum.json.GsonCreator;

/**
 * @author Hyunsik Choi
 *
 */
public class StringDatum extends Datum { 
	@Expose
	String val;
	
	/**
	 * @param type
	 */
	public StringDatum(String val) {
		super(DatumType.STRING);
		this.val = val;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asBool()
	 */
	@Override
	public boolean asBool() {	
		throw new InvalidCastException();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByte()
	 */
	@Override
	public byte asByte() {
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
		int res;
		try {
			res = Integer.valueOf(val);
		} catch (Exception e) {
			throw new InvalidCastException();
		}
		return res;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asLong()
	 */
	@Override
	public long asLong() {
		long res;
		try {
			res = Long.valueOf(val);
		} catch (Exception e) {
			throw new InvalidCastException();
		}
		return res;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByteArray()
	 */
	@Override
	public byte[] asByteArray() {		
		return val.getBytes();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asFloat()
	 */
	@Override
	public float asFloat() {
		float res;
		try {
			res = Float.valueOf(val);
		} catch (Exception e) {
			throw new InvalidCastException();
		}
		return res;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asDouble()
	 */
	@Override
	public double asDouble() {
		double res;
		try {
			res = Double.valueOf(val);
		} catch (Exception e) {
			throw new InvalidCastException();
		}
		return res;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asChars()
	 */
	@Override
	public String asChars() {
		return val;
	}

  public boolean equals(Object obj) {
    if (obj instanceof StringDatum) {
      return this.val.equals(((StringDatum) obj).val);
    }

    return false;
  }

  @Override
  public int size() {
    return val.getBytes().length;
  }
  
  @Override
  public int hashCode() {
    return val.hashCode();
  }
  
  // Datum Comparable
  public BoolDatum equalsTo(Datum datum) {
    switch (datum.type()) {
    case STRING:
      return DatumFactory
          .createBool(this.val.equals(((StringDatum) datum).val));
    default:
      throw new InvalidOperationException(datum.type());
    }
  }

  @Override
  public int compareTo(Datum datum) {
    switch (datum.type()) {
    case STRING:
      return this.val.compareTo(((StringDatum) datum).val);
    default:
      throw new InvalidOperationException(datum.type());
    }
  }

  public String toJSON() {
		return GsonCreator.getInstance().toJson(this, Datum.class);
	}
}
