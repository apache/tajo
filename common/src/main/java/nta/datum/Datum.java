package nta.datum;

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
	
	public abstract boolean asBool();
	public abstract byte asByte();
	public abstract short asShort();
	public abstract int asInt();
	public abstract long asLong();	
	public abstract byte [] asByteArray();
	public abstract float asFloat();
	public abstract double asDouble();
	public abstract String asChars();
	
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
		BoolDatum bd = (BoolDatum)this.lessThanEqual(o);
		if (bd.asBool()) {
			bd = (BoolDatum)this.greaterThanEqual(o);
			if (bd.asBool()) {
				return 0;
			} else {
				return -1;
			}
		} else {
			return 1;
		}
	}
}
