package nta.storage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author jimin
 * @author Hyunsik Choi
 * 
 */
public class MemTuple implements Tuple {
	public Map<Integer, Object> tupleData = new HashMap<Integer, Object>();

	//////////////////////////////////////////////////////
	// Getter
	//////////////////////////////////////////////////////

	public boolean contains(int fieldid) {
		return tupleData.containsKey(fieldid);
	}
	
	public void putBoolean(int fieldId, boolean value) {
		tupleData.put(fieldId, value);
	}
	
	public void putByte(int fieldId, byte value) {
		tupleData.put(fieldId, value);
	}

	public void putBytes(int fieldId, byte[] value) {
		tupleData.put(fieldId, value);
	}

	public void putBytes(int fieldId, ByteBuffer value) {
		tupleData.put(fieldId, value);
	}

	public void putShort(int fieldId, int value) {
		tupleData.put(fieldId, value);
	}

	public void putInt(int fieldId, int value) {
		tupleData.put(fieldId, value);
	}

	public void putLong(int fieldId, long value) {
		tupleData.put(fieldId, value);
	}

	public void putFloat(int fieldId, float value) {
		tupleData.put(fieldId, value);
	}

	public void putDouble(int fieldId, double value) {
		tupleData.put(fieldId, value);
	}

	public void putIPv4(int fieldId, InetAddress value) {
		tupleData.put(fieldId, value);
	}

	public void putIPv4(int fieldId, byte[] value) throws UnknownHostException {
		tupleData.put(fieldId, InetAddress.getByAddress(value));
	}

	public void putIPv6(int fieldId, InetAddress value) {
		tupleData.put(fieldId, value);
	}

	public void putString(int fieldId, String value) {
		tupleData.put(fieldId, value);
	}
	
	public void putString(int fieldId, char[] value) {
		this.putString(fieldId, new String(value));
	}
	
	public void putString(int fieldId, byte[] value) {
		this.putString(fieldId, new String(value));
	}
	
	//////////////////////////////////////////////////////
	// Getter
	//////////////////////////////////////////////////////
	@Override
	public boolean getBoolean(int fieldId) {
		return (Boolean) tupleData.get(fieldId);
	}

	public byte getByte(int fieldId) {
		return (Byte) tupleData.get(fieldId);
	}

	public byte[] getBytes(int fieldId) {
		return (byte[]) tupleData.get(fieldId);
	}

	public short getShort(int fieldId) {
		return (Short) tupleData.get(fieldId);
	}

	public int getInt(int fieldId) {
		return (Integer) tupleData.get(fieldId);
	}

	public long getLong(int fieldId) {
		return (Long) tupleData.get(fieldId);
	}

	public float getFloat(int fieldId) {
		return (Float) tupleData.get(fieldId);
	}

	public double getDouble(int fieldId) {
		return (Double) tupleData.get(fieldId);
	}

	public InetAddress getIPv4(int fieldId) {
		return (InetAddress) tupleData.get(fieldId);
	}

	public byte[] getIPv4Bytes(int fieldId) {
		return ((InetAddress) tupleData.get(fieldId)).getAddress();
	}

	public InetAddress getIPv6(int fieldId) {
		return (InetAddress) tupleData.get(fieldId);
	}

	public byte[] getIPv6Bytes(int fieldId) {
		return ((InetAddress) tupleData.get(fieldId)).getAddress(); 
	}

	public String getString(int fieldId) {
		return (String) tupleData.get(fieldId);
	}

	public String toString() {
		StringBuilder str = new StringBuilder();
		for (Entry<Integer, Object> e : tupleData.entrySet()) {
			str.append(e.getValue()).append("|");
		}
		return str.toString();
	}

	public boolean equals(Object o) {
		MemTuple other = (MemTuple) o;

		boolean match = true;
		for (Entry<Integer, Object> e : tupleData.entrySet()) {
			match = other.tupleData.get(e.getKey()).equals(e.getValue());
			if (match == false)
				return false;
		}

		return match;
	}
}
