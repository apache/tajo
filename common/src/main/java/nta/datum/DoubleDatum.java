/**
 * 
 */
package nta.datum;

import java.nio.ByteBuffer;

import nta.datum.exception.InvalidCastException;
import nta.datum.exception.InvalidOperationException;

/**
 * @author Hyunsik Choi
 *
 */
public class DoubleDatum extends Datum {
  private static final int size = 8;
  
	double val;
	
	/**
	 * 
	 */
	public DoubleDatum() {
		super(DatumType.DOUBLE);
	}
	
	public DoubleDatum(double val) {
		this();
		this.val = val;
	}
	
	public boolean asBool() {
		throw new InvalidCastException();
	}
	
	@Override
	public short asShort() {	
		return (short) val;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asInt()
	 */
	public int asInt() {		
		return (int) val;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asLong()
	 */
	public long asLong() {
		return (long) val;
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByte()
	 */
	public byte asByte() {
		throw new InvalidCastException();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asByteArray()
	 */
	public byte[] asByteArray() {
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putDouble(val);
		return bb.array();
	}

	/* (non-Javadoc)
	 * @see nta.common.datum.Datum#asFloat()
	 */
	public float asFloat() {		
		return (float) val;
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
		return ""+val;
	}

  @Override
  public int size() {
    return size;
  }
  
  @Override
  public int hashCode() {
    return (int) val;
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof DoubleDatum) {
      DoubleDatum other = (DoubleDatum) obj;
      return val == other.val;
    }
    
    return false;
  }
  
  // Datum Comparable
  public BoolDatum equalsTo(Datum datum) {
    switch (datum.type()) {
    case SHORT:
      return DatumFactory.createBool(val == datum.asShort());
    case INT:
      return DatumFactory.createBool(val == datum.asInt());
    case LONG:
      return DatumFactory.createBool(val == datum.asLong());
    case FLOAT:
      return DatumFactory.createBool(val == datum.asFloat());
    case DOUBLE:
      return DatumFactory.createBool(val == datum.asDouble());
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
  
  public BoolDatum lessThan(Datum datum) {
    switch (datum.type()) {
    case SHORT:
      return DatumFactory.createBool(val < datum.asShort());
    case INT:
      return DatumFactory.createBool(val < datum.asInt());
    case LONG:
      return DatumFactory.createBool(val < datum.asLong());
    case FLOAT:
      return DatumFactory.createBool(val < datum.asFloat());
    case DOUBLE:
      return DatumFactory.createBool(val < datum.asDouble());
    default:
      throw new InvalidOperationException();
    }
  }

  public BoolDatum lessThanEqual(Datum datum) {
    switch (datum.type()) {
    case SHORT:
      return DatumFactory.createBool(val <= datum.asShort());
    case INT:
      return DatumFactory.createBool(val <= datum.asInt());
    case LONG:
      return DatumFactory.createBool(val <= datum.asLong());
    case FLOAT:
      return DatumFactory.createBool(val <= datum.asFloat());
    case DOUBLE:
      return DatumFactory.createBool(val <= datum.asDouble());
    default:
      throw new InvalidOperationException(datum.toString());
    }
  }
  
  public BoolDatum greaterThan(Datum datum) {
    switch (datum.type()) {
    case SHORT:
      return DatumFactory.createBool(val > datum.asShort());
    case INT:
      return DatumFactory.createBool(val > datum.asInt());
    case LONG:
      return DatumFactory.createBool(val > datum.asLong());
    case FLOAT:
      return DatumFactory.createBool(val > datum.asFloat());
    case DOUBLE:
      return DatumFactory.createBool(val > datum.asDouble());
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
  
  public BoolDatum greaterThanEqual(Datum datum) {
    switch (datum.type()) {
    case SHORT:
      return DatumFactory.createBool(val >= datum.asShort());
    case INT:
      return DatumFactory.createBool(val >= datum.asInt());
    case LONG:
      return DatumFactory.createBool(val >= datum.asLong());
    case FLOAT:
      return DatumFactory.createBool(val >= datum.asFloat());
    case DOUBLE:
      return DatumFactory.createBool(val >= datum.asDouble());
    default:
      throw new InvalidOperationException();
    }
  }
  
  public Datum plus(Datum datum) {
    switch (datum.type()) {
    case SHORT:
      return DatumFactory.createDouble(val + datum.asShort());
    case INT:
      return DatumFactory.createDouble(val + datum.asInt());
    case LONG:
      return DatumFactory.createDouble(val + datum.asLong());
    case FLOAT:
      return DatumFactory.createDouble(val + datum.asFloat());
    case DOUBLE:
      return DatumFactory.createDouble(val + datum.asDouble());
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
  
  public Datum minus(Datum datum) {
    switch (datum.type()) {
    case SHORT:
      return DatumFactory.createDouble(val - datum.asShort());
    case INT:
      return DatumFactory.createDouble(val - datum.asInt());
    case LONG:
      return DatumFactory.createDouble(val - datum.asLong());
    case FLOAT:
      return DatumFactory.createDouble(val - datum.asFloat());
    case DOUBLE:
      return DatumFactory.createDouble(val - datum.asDouble());
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
  
  public Datum multiply(Datum datum) {
    switch (datum.type()) {
    case SHORT:
      return DatumFactory.createDouble(val * datum.asShort());
    case INT:
      return DatumFactory.createDouble(val * datum.asInt());
    case LONG:
      return DatumFactory.createDouble(val * datum.asLong());
    case FLOAT:
      return DatumFactory.createDouble(val * datum.asFloat());
    case DOUBLE:
      return DatumFactory.createDouble(val * datum.asDouble());
    default:
      throw new InvalidOperationException();
    }
  }
  
  public Datum divide(Datum datum) {
    switch (datum.type()) {
    case SHORT:
      return DatumFactory.createDouble(val / datum.asShort());
    case INT:
      return DatumFactory.createDouble(val / datum.asInt());
    case LONG:
      return DatumFactory.createDouble(val / datum.asLong());
    case FLOAT:
      return DatumFactory.createDouble(val / datum.asFloat());
    case DOUBLE:
      return DatumFactory.createDouble(val / datum.asDouble());
    default:
      throw new InvalidOperationException(datum.type());
    }
  }
}
