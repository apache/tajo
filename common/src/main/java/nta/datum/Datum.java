package nta.datum;

import nta.datum.exception.InvalidOperationException;

/**
 * @author Hyunsik Choi
 *
 */
public abstract class Datum implements Comparable<Datum> {
	private DatumType type;
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
	
	public abstract int size();
	
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
