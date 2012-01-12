package nta.storage;

import java.net.InetAddress;

/** 
 * 
 * @author jimin
 * @author Hyunsik Choi
 * 
 */

public interface Tuple {
	
	public int size();
	
	public boolean contains(int fieldid);
	
	public void clear();
	
	public void put(int fieldId, Object value);
	
	public void put(Object...values);
	
	public Object get(int fieldId);

	public boolean getBoolean(int fieldId);
	
	public byte getByte(int fieldId);
	
	public byte [] getBytes(int fieldId);
	
	public short getShort(int fieldId);
	
	public int getInt(int fieldId);
	
	public long getLong(int fieldId);
	
	public float getFloat(int fieldId);
	
	public double getDouble(int fieldId);
	
	public InetAddress getIPv4(int fieldId);
	
	public byte [] getIPv4Bytes(int fieldId);
	
	public InetAddress getIPv6(int fieldId);
	
	public byte [] getIPv6Bytes(int fieldId);
	
	public String getString(int fieldId);
}
