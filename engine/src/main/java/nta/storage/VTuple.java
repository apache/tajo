package nta.storage;

import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Hyunsik Choi
 * 
 */
public class VTuple implements Tuple {
	private static Log LOG = LogFactory.getLog(VTuple.class);
	
	public Object [] values;
	
	public VTuple(int size) {
		values = new Object [size];
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
	public void put(int fieldId, Object value) {
		values[fieldId] = value;
	}
	
	public void put(Object...values) {
	  for(int i=0; i < values.length; i++) {
	    values[i] = values[i];
	  }
	}
	
	//////////////////////////////////////////////////////
	// Getter
	//////////////////////////////////////////////////////
	public Object get(int fieldId) {
		return this.values[fieldId];
	}
	
	@Override
	public boolean getBoolean(int fieldId) {
		return (Boolean) values[fieldId];
	}

	public byte getByte(int fieldId) {
		return (Byte) values[fieldId];
	}

	public byte[] getBytes(int fieldId) {
		return (byte[]) values[fieldId];
	}

	public short getShort(int fieldId) {
		return (Short) values[fieldId];
	}

	public int getInt(int fieldId) {		
		return (Integer) values[fieldId];			
	}

	public long getLong(int fieldId) {
		return (Long) values[fieldId];
	}

	public float getFloat(int fieldId) {
		return (Float) values[fieldId];
	}

	public double getDouble(int fieldId) {
		return (Double) values[fieldId];
	}

	public InetAddress getIPv4(int fieldId) {
//		return (InetAddress) values[fieldId];
		return null;
	}

	public byte[] getIPv4Bytes(int fieldId) {
//		return ((InetAddress) values[fieldId]).getAddress();
		return (byte[])values[fieldId];
	}

	public InetAddress getIPv6(int fieldId) {
//		return (InetAddress) values[fieldId];
		return null;
	}

	public byte[] getIPv6Bytes(int fieldId) {
//		return ((InetAddress) values[fieldId]).getAddress();
		return (byte[])values[fieldId];
	}

	public String getString(int fieldId) {
		return (String) values[fieldId];
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

	public boolean equals(Object o) {
		VTuple other = (VTuple) o;
		
		for(int i=0; i < values.length; i++) {
			if(values[i] != other.values[i] ||
					(values[i].equals(other.values[i]) == false))
					return false;			                       
		}

		return true;
	}
}
