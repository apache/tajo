/**
 * 
 */
package nta.storage;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.ByteBuffer;

import nta.catalog.Schema;

/**
 * @author Hyunsik Choi
 *
 */
public interface UpdatableScanner extends Scanner {
	abstract public void putAsByte(int fid, byte val) throws IOException;

	abstract public void putAsShort(int fid, short val) throws IOException;

	abstract public void putAsInt(int fid, int val) throws IOException;

	abstract public void putAsLong(int fid, long val) throws IOException;

	abstract public void putAsFloat(int fid, float val) throws IOException;

	abstract public void putAsDouble(int fid, double val) throws IOException;

	abstract public void putAsBytes(int fid, byte [] val) throws IOException;

	abstract public void putAsBytes(int fid, ByteBuffer val) throws IOException;

	abstract public void putAsIPv4(int fid, Inet4Address val) throws IOException;

	abstract public void putAsIPv4(int fid, byte [] val) throws Exception;

	abstract public void putAsIPv6(int fid, Inet6Address val) throws IOException;

	abstract public void putAsChars(int fid, char [] val) throws IOException;

	abstract public void putAsChars(int fid, String val) throws IOException;

	abstract public void putAsChars(int fid, byte [] val) throws IOException;

	abstract public void addTuple(Tuple tuple) throws IOException;
}