/**
 * 
 */
package nta.engine;

import nta.engine.exception.NTAQueryException;

/**
 * @author Hyunsik Choi
 *
 */
public interface ResultSet {
	 
	public boolean next();
	 
	public boolean previous();
	
	boolean first();
	
	boolean last();
	
	int getRow();
	
	public void close();
	
	////////////////////////////////////
	// Getter
	////////////////////////////////////
	
	String getString(int columnIndex) throws NTAQueryException;
	
	boolean getBoolean(int columnIndex) throws NTAQueryException;
	
	byte getByte(int columnIndex) throws NTAQueryException;
	
	short getShort(int columnIndex) throws NTAQueryException;
	
	int getInt(int columnIndex) throws NTAQueryException;
	
	long getLong(int columnIndex) throws NTAQueryException;
	
	float getFloat(int columnIndex) throws NTAQueryException;
	
	double getDouble(int columnIndex) throws NTAQueryException;
	
	byte[] getBytes(int columnIndex) throws NTAQueryException;
	
	String getString(String columnLabel) throws NTAQueryException;
	
	boolean getBoolean(String columnLabel) throws NTAQueryException;
	
	byte getByte(String columnLabel) throws NTAQueryException;
	
	short getShort(String columnLabel) throws NTAQueryException;
	
	int getInt(String columnLabel) throws NTAQueryException;
	
	long getLong(String columnLabel) throws NTAQueryException;
	
	float getFloat(String columnLabel) throws NTAQueryException;
	
	double getDouble(String columnLabel) throws NTAQueryException;
	
	byte[] getBytes(String columnLabel) throws NTAQueryException;
}
