package nta.storage;

import java.net.InetAddress;

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
	
	public void put(int fieldId, Datum value);
	
	public void put(Datum...values);
	
	public Datum get(int fieldId);
	
	public void setOffset(long offset);
	
	public long getOffset();

	public BoolDatum getBoolean(int fieldId);
	
	public ByteDatum getByte(int fieldId);
	
	public BytesDatum getBytes(int fieldId);
	
	public ShortDatum getShort(int fieldId);
	
	public IntDatum getInt(int fieldId);
	
	public LongDatum getLong(int fieldId);
	
	public FloatDatum getFloat(int fieldId);
	
	public DoubleDatum getDouble(int fieldId);
	
	public IPv4Datum getIPv4(int fieldId);
	
	public byte [] getIPv4Bytes(int fieldId);
	
	public InetAddress getIPv6(int fieldId);
	
	public byte [] getIPv6Bytes(int fieldId);
	
	public StringDatum getString(int fieldId);
}
