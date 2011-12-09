package nta.storage;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import nta.catalog.Column;
import nta.catalog.Schema;

/**
 * @author Hyunsik Choi
 *
 */
public class MemTable implements UpdatableScanner {
	private final Store store;
	private List<VTuple> slots;
	private int cur = -1;
	private boolean hasRead = true;
	
	
	/**
	 * 
	 */
	public MemTable(Store store) {
		this.store = store;
		slots = new ArrayList<VTuple>();
	}
	
	public MemTable(Store store, int initialCapacity) {
		this(store);
		slots = new ArrayList<VTuple>(initialCapacity);
	}
	
	public MemTable(MemTable memSlots) {
		this(memSlots.store);
		this.slots = memSlots.slots; 
	}
	
	@Override
	public void init() throws IOException {
	}

	/* (non-Javadoc)
	 * @see nta.query.executor.ScanExec#hasNextTuple()
	 */
	@Override
	public Tuple next() throws IOException {	
		cur++;
		
		if(cur < slots.size()) {
			return slots.get(cur);
		} else
			return null;
	}
	

	@Override
	public VTuple next2() throws IOException {
		cur++;		
		if(cur < slots.size()) {
			Tuple t = slots.get(cur);
			VTuple tuple = new VTuple(store.getSchema().getColumnNum());
			
			Column field = null;
			for(int i=0; i < store.getSchema().getColumnNum(); i++) {
				field = store.getSchema().getColumn(i);

				switch (field.getDataType()) {
				case BYTE:
					tuple.put(i, t.getByte(i));
					break;
				case STRING:					
					tuple.put(i, t.getString(i));
					break;
				case SHORT:
					tuple.put(i, t.getShort(i));
					break;
				case INT:
					tuple.put(i, t.getInt(i));
					break;
				case LONG:
					tuple.put(i, t.getLong(i));
					break;
				case FLOAT:
					tuple.put(i, t.getFloat(i));
					break;
				case DOUBLE:
					tuple.put(i, t.getDouble(i));
					break;
				case IPv4:
					tuple.put(i, t.getIPv4(i));
					break;
				case IPv6:
					tuple.put(i, t.getIPv6(i));
					break;
				default:
					;
				}				
			}			
			
			return tuple;
		} else
			return null;
	}

	@Override
	public void reset() {
		hasRead = true;
		cur=-1;
	}
	
	public void copyFromCollection(Collection<VTuple> tuples) {
		this.slots.addAll(tuples);
	}

	@Override
	public void putAsByte(int fieldId, byte value) {
		slots.get(cur).put(fieldId, value);
	}

	@Override
	public void putAsShort(int fieldId, short value) {
		slots.get(cur).put(fieldId, value);
	}

	@Override
	public void putAsInt(int fieldId, int value) {
		slots.get(cur).put(fieldId, value);
		
	}

	@Override
	public void putAsLong(int fieldId, long value) {
		slots.get(cur).put(fieldId, value);		
	}

	@Override
	public void putAsFloat(int fieldId, float value) {
		slots.get(cur).put(fieldId, value);
	}

	@Override
	public void putAsDouble(int fieldId, double value) {
		slots.get(cur).put(fieldId, value);		
	}

	@Override
	public void putAsBytes(int fieldId, byte[] value) {
		
	}

	@Override
	public void putAsBytes(int fid, ByteBuffer val) {
				
	}

	@Override
	public void putAsIPv4(int fieldId, Inet4Address value) {
		slots.get(cur).put(fieldId, value);		
	}

	@Override
	public void putAsIPv4(int fid, byte[] val) {
				
	}

	@Override
	public void putAsIPv6(int fid, Inet6Address val) {
				
	}
	
	@Override
	public void putAsChars(int fieldId, char [] value) {
		slots.get(cur).put(fieldId, new String(value));
	}

	@Override
	public void putAsChars(int fieldId, String value) {
		slots.get(cur).put(fieldId, value);
	}

	@Override
	public void putAsChars(int fieldId, byte[] value) {
		slots.get(cur).put(fieldId, new String(value));
	}

	@Override
	public void addTuple(Tuple tuple) throws IOException {
		slots.add((VTuple) tuple);
	}
	
	@Override
	public Schema getSchema() {
		return store.getSchema();
	}
	
	////////////////////////////////////////////////////////////////
	// Storage Implementation
	////////////////////////////////////////////////////////////////
	@Override
	public boolean isLocalFile() {
		return true;
	}

	@Override
	public boolean readOnly() {
		return false;
	}

	@Override
	public boolean canRandomAccess() {
		return true;
	}

	@Override
	public void close() {		
		slots.clear();
		slots = null;
	}
}
