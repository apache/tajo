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
	
	@Override
	public String toString() {
		return asChars();
	}
	
	public Datum plus(Datum datum) {
		return plus(this,datum);
	}
	public Datum minus(Datum datum) {
		return minus(this,datum);
	}
	public Datum multiply(Datum datum) {
		return multiply(this,datum);
	}
	public Datum divide(Datum datum) {
		return divide(this,datum);
	}
	
	@Override
	public boolean equals(Object o) {
		return equalTo(this, (Datum)o).asBool();
	}	
	public Datum equalTo(Datum datum) {
		return equalTo(this, datum);
	}	
	public Datum lessThan(Datum datum) {
		return lessThan(this, datum);
	}	
	public Datum lessThanEqual(Datum datum) {
		return lessThanOrEqualTo(this,datum);
	}	
	public Datum greaterThan(Datum datum) {
		return greaterThan(this,datum);
	}	
	public Datum greaterThanEqual(Datum datum) {
		return greaterThanOrEqualTo(this,datum);
	}
	
	public static Datum plus(Datum d1, Datum d2) {
		switch(d1.type()) {
		case INT:		
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asInt() + d2.asInt());
			case LONG: return DatumFactory.create(d1.asInt() + d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asInt() + d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asInt() + d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case LONG:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asLong() + d2.asInt());
			case LONG: return DatumFactory.create(d1.asLong() + d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asLong() + d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asLong() + d2.asDouble());
			default : throw new InvalidOperationException();
			}
		
		case FLOAT:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asFloat() + d2.asInt());
			case LONG: return DatumFactory.create(d1.asFloat() + d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asFloat() + d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asFloat() + d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case DOUBLE:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asDouble() + d2.asInt());
			case LONG: return DatumFactory.create(d1.asDouble() + d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asDouble() + d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asDouble() + d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		default: throw new InvalidOperationException();
		}
	}
	
	public static Datum minus(Datum d1, Datum d2) {
		switch(d1.type()) {
		case INT:		
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asInt() - d2.asInt());
			case LONG: return DatumFactory.create(d1.asInt() - d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asInt() - d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asInt() - d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case LONG:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asLong() - d2.asInt());
			case LONG: return DatumFactory.create(d1.asLong() - d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asLong() - d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asLong() - d2.asDouble());
			default : throw new InvalidOperationException();
			}
		
		case FLOAT:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asFloat() - d2.asInt());
			case LONG: return DatumFactory.create(d1.asFloat() - d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asFloat() - d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asFloat() - d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case DOUBLE:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asDouble() - d2.asInt());
			case LONG: return DatumFactory.create(d1.asDouble() - d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asDouble() - d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asDouble() - d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		default: throw new InvalidOperationException();
		}
	}
	
	public static Datum multiply(Datum one, Datum by) {
		switch(one.type()) {
		case INT:		
			switch(by.type()) {
			case INT: return DatumFactory.create(one.asInt() * by.asInt());
			case LONG: return DatumFactory.create(one.asInt() * by.asLong());
			case FLOAT: return DatumFactory.create(one.asInt() * by.asFloat());
			case DOUBLE: return DatumFactory.create(one.asInt() * by.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case LONG:
			switch(by.type()) {
			case INT: return DatumFactory.create(one.asLong() * by.asInt());
			case LONG: return DatumFactory.create(one.asLong() * by.asLong());
			case FLOAT: return DatumFactory.create(one.asLong() * by.asFloat());
			case DOUBLE: return DatumFactory.create(one.asLong() * by.asDouble());
			default : throw new InvalidOperationException();
			}
		
		case FLOAT:
			switch(by.type()) {
			case INT: return DatumFactory.create(one.asFloat() * by.asInt());
			case LONG: return DatumFactory.create(one.asFloat() * by.asLong());
			case FLOAT: return DatumFactory.create(one.asFloat() * by.asFloat());
			case DOUBLE: return DatumFactory.create(one.asFloat() * by.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case DOUBLE:
			switch(by.type()) {
			case INT: return DatumFactory.create(one.asDouble() * by.asInt());
			case LONG: return DatumFactory.create(one.asDouble() * by.asLong());
			case FLOAT: return DatumFactory.create(one.asDouble() * by.asFloat());
			case DOUBLE: return DatumFactory.create(one.asDouble() * by.asDouble());
			default : throw new InvalidOperationException();
			}
			
		default: throw new InvalidOperationException();
		}
	}
	
	public static Datum divide(Datum one, Datum by) {
		switch(one.type()) {
		case INT:		
			switch(by.type()) {
			case INT: return DatumFactory.create(one.asInt() / by.asInt());
			case LONG: return DatumFactory.create(one.asInt() / by.asLong());
			case FLOAT: return DatumFactory.create(one.asInt() / by.asFloat());
			case DOUBLE: return DatumFactory.create(one.asInt() / by.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case LONG:
			switch(by.type()) {
			case INT: return DatumFactory.create(one.asLong() / by.asInt());
			case LONG: return DatumFactory.create(one.asLong() / by.asLong());
			case FLOAT: return DatumFactory.create(one.asLong() / by.asFloat());
			case DOUBLE: return DatumFactory.create(one.asLong() / by.asDouble());
			default : throw new InvalidOperationException();
			}
		
		case FLOAT:
			switch(by.type()) {
			case INT: return DatumFactory.create(one.asFloat() / by.asInt());
			case LONG: return DatumFactory.create(one.asFloat() / by.asLong());
			case FLOAT: return DatumFactory.create(one.asFloat() / by.asFloat());
			case DOUBLE: return DatumFactory.create(one.asFloat() / by.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case DOUBLE:
			switch(by.type()) {
			case INT: return DatumFactory.create(one.asDouble() / by.asInt());
			case LONG: return DatumFactory.create(one.asDouble() / by.asLong());
			case FLOAT: return DatumFactory.create(one.asDouble() / by.asFloat());
			case DOUBLE: return DatumFactory.create(one.asDouble() / by.asDouble());
			default : throw new InvalidOperationException();
			}
			
		default: throw new InvalidOperationException();
		}
	}
	
	public static Datum equalTo(Datum d1, Datum d2) {
		switch(d1.type()) {
		case INT:		
			switch(d2.type()) { 
			case INT: return DatumFactory.create(d1.asInt() == d2.asInt());
			case LONG: return DatumFactory.create(d1.asInt() == d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asInt() == d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asInt() == d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case LONG:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asLong() == d2.asInt());
			case LONG: return DatumFactory.create(d1.asLong() == d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asLong() == d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asLong() == d2.asDouble());
			default : throw new InvalidOperationException();
			}
		
		case FLOAT:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asFloat() == d2.asInt());
			case LONG: return DatumFactory.create(d1.asFloat() == d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asFloat() == d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asFloat() == d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case DOUBLE:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asDouble() == d2.asInt());
			case LONG: return DatumFactory.create(d1.asDouble() == d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asDouble() == d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asDouble() == d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		default: throw new InvalidOperationException();
		}
	}
	
	public static Datum lessThan(Datum d1, Datum d2) {
		switch(d1.type()) {
		case INT:		
			switch(d2.type()) { 
			case INT: return DatumFactory.create(d1.asInt() < d2.asInt());
			case LONG: return DatumFactory.create(d1.asInt() < d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asInt() < d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asInt() < d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case LONG:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asLong() < d2.asInt());
			case LONG: return DatumFactory.create(d1.asLong() < d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asLong() < d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asLong() < d2.asDouble());
			default : throw new InvalidOperationException();
			}
		
		case FLOAT:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asFloat() < d2.asInt());
			case LONG: return DatumFactory.create(d1.asFloat() < d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asFloat() < d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asFloat() < d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case DOUBLE:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asDouble() < d2.asInt());
			case LONG: return DatumFactory.create(d1.asDouble() < d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asDouble() < d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asDouble() < d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		default: throw new InvalidOperationException();
		}
	}
	
	public static Datum lessThanOrEqualTo(Datum d1, Datum d2) {
		switch(d1.type()) {
		case INT:		
			switch(d2.type()) { 
			case INT: return DatumFactory.create(d1.asInt() <= d2.asInt());
			case LONG: return DatumFactory.create(d1.asInt() <= d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asInt() <= d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asInt() <= d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case LONG:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asLong() <= d2.asInt());
			case LONG: return DatumFactory.create(d1.asLong() <= d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asLong() <= d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asLong() <= d2.asDouble());
			default : throw new InvalidOperationException();
			}
		
		case FLOAT:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asFloat() <= d2.asInt());
			case LONG: return DatumFactory.create(d1.asFloat() <= d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asFloat() <= d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asFloat() <= d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case DOUBLE:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asDouble() <= d2.asInt());
			case LONG: return DatumFactory.create(d1.asDouble() <= d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asDouble() <= d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asDouble() <= d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		default: throw new InvalidOperationException();
		}
	}
	
	public static Datum greaterThan(Datum d1, Datum d2) {
		switch(d1.type()) {
		case INT:		
			switch(d2.type()) { 
			case INT: return DatumFactory.create(d1.asInt() > d2.asInt());
			case LONG: return DatumFactory.create(d1.asInt() > d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asInt() > d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asInt() > d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case LONG:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asLong() > d2.asInt());
			case LONG: return DatumFactory.create(d1.asLong() > d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asLong() > d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asLong() > d2.asDouble());
			default : throw new InvalidOperationException();
			}
		
		case FLOAT:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asFloat() > d2.asInt());
			case LONG: return DatumFactory.create(d1.asFloat() > d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asFloat() > d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asFloat() > d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case DOUBLE:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asDouble() > d2.asInt());
			case LONG: return DatumFactory.create(d1.asDouble() > d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asDouble() > d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asDouble() > d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		default: throw new InvalidOperationException();
		}
	}
	
	public static Datum greaterThanOrEqualTo(Datum d1, Datum d2) {
		switch(d1.type()) {
		case INT:		
			switch(d2.type()) { 
			case INT: return DatumFactory.create(d1.asInt() >= d2.asInt());
			case LONG: return DatumFactory.create(d1.asInt() >= d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asInt() >= d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asInt() >= d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case LONG:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asLong() >= d2.asInt());
			case LONG: return DatumFactory.create(d1.asLong() >= d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asLong() >= d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asLong() >= d2.asDouble());
			default : throw new InvalidOperationException();
			}
		
		case FLOAT:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asFloat() >= d2.asInt());
			case LONG: return DatumFactory.create(d1.asFloat() >= d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asFloat() >= d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asFloat() >= d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		case DOUBLE:
			switch(d2.type()) {
			case INT: return DatumFactory.create(d1.asDouble() >= d2.asInt());
			case LONG: return DatumFactory.create(d1.asDouble() >= d2.asLong());
			case FLOAT: return DatumFactory.create(d1.asDouble() >= d2.asFloat());
			case DOUBLE: return DatumFactory.create(d1.asDouble() >= d2.asDouble());
			default : throw new InvalidOperationException();
			}
			
		default: throw new InvalidOperationException();
		}
	}
	
	@Override
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
