package nta.datum;

import nta.datum.exception.InvalidCastException;
import nta.datum.exception.InvalidOperationException;

import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 *
 */
public abstract class Datum implements Comparable<Datum> {
	@Expose
	private DatumType type;
	
	@SuppressWarnings("unused")
  private Datum() {
	}
	
	public Datum(DatumType type) {
		this.type = type;
	}
	
	public DatumType type() {
		return this.type;
	}
	
	public boolean asBool() {
    throw new InvalidCastException(type + " cannot be casted to BOOL type");
  }

  public byte asByte() {
    throw new InvalidCastException(type + " cannot be casted to BYTE type");
  }

  public char asChar() {
    throw new InvalidCastException(type + " cannot be casted to CHAR type");
  }

	public short asShort() {
    throw new InvalidCastException(type + " cannot be casted to SHORT type");
  }
	public int asInt() {
    throw new InvalidCastException(type + " cannot be casted to INT type");
  }

  public long asLong() {
    throw new InvalidCastException(type + " cannot be casted to LONG type");
  }

	public byte [] asByteArray() {
    throw new InvalidCastException(type + " cannot be casted to BYTES type");
  }

	public float asFloat() {
    throw new InvalidCastException(type + " cannot be casted to FLOAT type");
  }

	public double asDouble() {
    throw new InvalidCastException(type + " cannot be casted to DOUBLE type");
  }

	public String asChars() {
    throw new InvalidCastException(type + " cannot be casted to STRING type");
  }
	
	public boolean isNumeric() {
	  return isNumber() || isReal();
	}
	
	public boolean isNumber() {
	  return 
	      this.type == DatumType.SHORT ||
	      this.type == DatumType.INT ||
	      this.type == DatumType.LONG;
	}
	
	public boolean isReal() {
    return 
        this.type == DatumType.FLOAT ||
        this.type == DatumType.DOUBLE;
  }
	
	public abstract int size();
	
	public abstract String toJSON();
	
	@Override
	public String toString() {
		return asChars();
	}
	
	public Datum plus(Datum datum) {
	  throw new InvalidOperationException(datum.type);
	}
	
	public Datum minus(Datum datum) {
	  throw new InvalidOperationException(datum.type);
	}
	
	public Datum multiply(Datum datum) {
	  throw new InvalidOperationException(datum.type);
	}
	
	public Datum divide(Datum datum) {
	  throw new InvalidOperationException(datum.type);
	}
	
	public BoolDatum equalsTo(Datum datum) {
	  throw new InvalidOperationException(datum.type);
	}
	public BoolDatum lessThan(Datum datum) {
	  throw new InvalidOperationException(datum.type);
	}
	
	public BoolDatum lessThanEqual(Datum datum) {
	  throw new InvalidOperationException(datum.type);
	}	
	
	public BoolDatum greaterThan(Datum datum) {
	  throw new InvalidOperationException(datum.type);
	}
	
	public BoolDatum greaterThanEqual(Datum datum) {
	  throw new InvalidOperationException(datum.type);
	}
	
	public int compareTo(Datum o) {
		BoolDatum bd = this.lessThan(o);
    if (bd.asBool()) {
      return -1;
    } else {
			bd = this.greaterThan(o);
			if (bd.asBool()) {
        return 1;
      } else {
        return 0;
      }
		}
	}
}
