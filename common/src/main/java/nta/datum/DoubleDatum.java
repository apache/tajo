/**
 * 
 */
package nta.datum;

import java.nio.ByteBuffer;

import com.google.gson.annotations.Expose;

import nta.datum.exception.InvalidOperationException;
import nta.datum.json.GsonCreator;

/**
 * @author Hyunsik Choi
 *
 */
public class DoubleDatum extends NumericDatum {
  private static final int size = 8;
  @Expose private double val;

	public DoubleDatum() {
		super(DatumType.DOUBLE);
	}
	
	public DoubleDatum(double val) {
		this();
		this.val = val;
	}
	
	@Override
	public short asShort() {	
		return (short) val;
	}

	@Override
	public int asInt() {		
		return (int) val;
	}

  @Override
	public long asLong() {
		return (long) val;
	}

  @Override
	public byte[] asByteArray() {
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putDouble(val);
		return bb.array();
	}

  @Override
	public float asFloat() {		
		return (float) val;
	}

  @Override
	public double asDouble() {
		return val;
	}

  @Override
	public String asChars() {
		return ""+val;
	}

  @Override
	public String toJSON() {
		return GsonCreator.getInstance().toJson(this, Datum.class);
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

  @Override
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

  @Override
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

  @Override
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

  @Override
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

  @Override
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

  @Override
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

  @Override
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

  @Override
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

  @Override
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

  @Override
  public Datum modular(Datum datum) {
    switch (datum.type()) {
      case SHORT:
        return DatumFactory.createDouble(val % datum.asShort());
      case INT:
        return DatumFactory.createDouble(val % datum.asInt());
      case LONG:
        return DatumFactory.createDouble(val % datum.asLong());
      case FLOAT:
        return DatumFactory.createDouble(val % datum.asFloat());
      case DOUBLE:
        return DatumFactory.createDouble(val % datum.asDouble());
      default:
        throw new InvalidOperationException(datum.type());
    }
  }
  
  @Override
  public void inverseSign() {   
    this.val = -val;
  }
}
