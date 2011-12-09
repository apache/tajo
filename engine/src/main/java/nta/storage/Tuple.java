package nta.storage;

import java.net.InetAddress;

import nta.common.type.IPv4;

/** 
 * 
 * @author jimin
 * @author Hyunsik Choi
 * 
 */

public interface Tuple {
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
	public boolean contains(int fieldid);
}
