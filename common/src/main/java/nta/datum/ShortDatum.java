package nta.datum;

import java.nio.ByteBuffer;

import nta.datum.exception.InvalidCastException;

public class ShortDatum extends Datum {
	int val;
	public ShortDatum(short val) {
		super(DatumType.SHORT);
		this.val = val;		
	}

	@Override
	public boolean asBool() {
		throw new InvalidCastException();
	}

	@Override
	public byte asByte() {
		throw new InvalidCastException();
	}
	
	@Override
	public short asShort() {	
		return (short) val;
	}

	@Override
	public int asInt() {
		return val;
	}

	@Override
	public long asLong() {
		return val;
	}

	@Override
	public byte[] asByteArray() {
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(val);
		return bb.array();
	}

	@Override
	public float asFloat() {
		return val;
	}

	@Override
	public double asDouble() {
		return val;
	}

	@Override
	public String asChars() {
		return ""+val;
	}

}
