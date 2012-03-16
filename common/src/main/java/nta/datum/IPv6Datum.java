package nta.datum;

import nta.common.exception.NotImplementedException;
import nta.datum.exception.InvalidCastException;
import nta.datum.json.GsonCreator;

public class IPv6Datum extends Datum {
  
	public IPv6Datum(DatumType type) {
		super(type);
	}

	@Override
	public boolean asBool() {
		return false;
	}

	@Override
	public byte asByte() {
		return 0;
	}
	
	@Override
	public short asShort() {	
		throw new InvalidCastException();
	}

	@Override
	public int asInt() {
		return 0;
	}

	@Override
	public long asLong() {
		return 0;
	}

	@Override
	public byte[] asByteArray() {
		return null;
	}

	@Override
	public float asFloat() {
		return 0;
	}

	@Override
	public double asDouble() {
		return 0;
	}

	@Override
	public String asChars() {
		return null;
	}

  @Override
  public int size() {
    return 16;
  }
  
  // Datum Comparable
  public BoolDatum equalsTo(Datum datum) {
    throw new NotImplementedException();
  }
  
  @Override
  public int compareTo(Datum datum) {
    throw new NotImplementedException();
  }
  
  public String toJSON() {
	  return GsonCreator.getInstance().toJson(this, Datum.class);
  }
}
