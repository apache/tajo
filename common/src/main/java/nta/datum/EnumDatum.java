package nta.datum;

public class EnumDatum extends Datum {

	public EnumDatum(DatumType type) {
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
		return 0;
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
    // TODO - to be improved
    return 1;
  }
}
