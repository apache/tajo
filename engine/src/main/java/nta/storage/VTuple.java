package nta.storage;

import java.net.InetAddress;
import java.util.Arrays;

import nta.datum.BoolDatum;
import nta.datum.ByteDatum;
import nta.datum.BytesDatum;
import nta.datum.Datum;
import nta.datum.DoubleDatum;
import nta.datum.FloatDatum;
import nta.datum.IPv4Datum;
import nta.datum.IntDatum;
import nta.datum.LongDatum;
import nta.datum.ShortDatum;
import nta.datum.StringDatum;
import nta.datum.exception.InvalidCastException;

/**
 * @author Hyunsik Choi
 * 
 */
public class VTuple implements Tuple {
	public Datum [] values;
	private long offset;
	
	public VTuple(int size) {
		values = new Datum [size];
	}	

	@Override
	public int size() {	
		return values.length;
	}
	
	public boolean contains(int fieldId) {
		return values[fieldId] != null;
	}
	
  @Override
  public void clear() {   
    for (int i=0; i < values.length; i++) {
      values[i] = null;
    }
  }
	
	//////////////////////////////////////////////////////
	// Setter
	//////////////////////////////////////////////////////	
	public void put(int fieldId, Datum value) {
		values[fieldId] = value;
	}
	
	public void put(Datum...values) {
	  for(int i=0; i < values.length; i++) {
	    this.values[i] = values[i];
	  }
	}
	
	//////////////////////////////////////////////////////
	// Getter
	//////////////////////////////////////////////////////
	public Datum get(int fieldId) {
		return this.values[fieldId];
	}
	
	public void setOffset(long offset) {
	  this.offset = offset;
	}
	
	public long getOffset() {
	  return this.offset;
	}
	
	@Override
	public BoolDatum getBoolean(int fieldId) {
		return (BoolDatum) values[fieldId];
	}

	public ByteDatum getByte(int fieldId) {
		return (ByteDatum) values[fieldId];
	}

	public BytesDatum getBytes(int fieldId) {
		return (BytesDatum) values[fieldId];
	}

	public ShortDatum getShort(int fieldId) {
		return (ShortDatum) values[fieldId];
	}

	public IntDatum getInt(int fieldId) {
		return (IntDatum) values[fieldId];			
	}

	public LongDatum getLong(int fieldId) {
		return (LongDatum) values[fieldId];
	}

	public FloatDatum getFloat(int fieldId) {
		return (FloatDatum) values[fieldId];
	}

	public DoubleDatum getDouble(int fieldId) {
		return (DoubleDatum) values[fieldId];
	}

	public IPv4Datum getIPv4(int fieldId) {
		return (IPv4Datum) values[fieldId];
	}

	public byte[] getIPv4Bytes(int fieldId) {
		return ((IPv4Datum)values[fieldId]).asByteArray();
	}

	public InetAddress getIPv6(int fieldId) {
		throw new InvalidCastException("IPv6 is unsupported yet");
	}

	public byte[] getIPv6Bytes(int fieldId) {
	  throw new InvalidCastException("IPv6 is unsupported yet");
	}

	public StringDatum getString(int fieldId) {
		return (StringDatum) values[fieldId];
	}

	public String toString() {
		boolean first = true;
		StringBuilder str = new StringBuilder();
		str.append("(");
		for(int i=0; i < values.length; i++) {			
			if(values[i] != null) {
				if(first) {
					first = false;
				} else {
					str.append(", ");
				}
				str.append(i)
				.append("=>")
				.append(values[i]);
			}
		}
		str.append(")");
		return str.toString();
	}
	
	@Override
	public int hashCode() {
	  int hashCode = 37;
	  for (int i=0; i < values.length; i++) {
	    if(values[i] != null) {
        hashCode ^= (values[i].hashCode() * 41);
	    } else {
	      hashCode = hashCode ^ (i + 17);
	    }
	  }
	  
	  return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof VTuple) {
	    VTuple other = (VTuple) obj;
	    return Arrays.equals(values, other.values);
		}
		
		return false;
	}
}
