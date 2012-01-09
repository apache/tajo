package nta.storage;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.engine.ipc.protocolrecords.Tablet;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Hyunsik Choi
 *
 */
public class MemTable implements Appender, FileScanner {
	private final TableMeta meta;
  private final URI uri;
	private List<VTuple> slots;
	private int cur = -1;
	private boolean hasRead = true;
	
	/**
	 * 
	 */
	public MemTable(TableMeta info, URI uri) {
	  this.meta = info;
		this.uri = uri;
		slots = new ArrayList<VTuple>();
	}
	
	public MemTable(TableMeta info, URI uri, int initialCapacity) {
	  this(info, uri);
		slots = new ArrayList<VTuple>(initialCapacity);
	}
	
	public MemTable(TableMeta info, URI uri, MemTable memTable) {
		this(info, uri);
		this.slots = memTable.slots; 
	}

	@Override
	public VTuple next() throws IOException {
		cur++;		
		if(cur < slots.size()) {
			Tuple t = slots.get(cur);
			VTuple tuple = new VTuple(meta.getSchema().getColumnNum());
			
			Column field = null;
			for(int i=0; i < meta.getSchema().getColumnNum(); i++) {
				field = meta.getSchema().getColumn(i);

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
	public void addTuple(Tuple tuple) throws IOException {
		slots.add((VTuple) tuple);
	}

	@Override
	public void close() {
	}

  @Override
  public void flush() throws IOException {
    // nothing    
  }

  @Override
  public void init(Configuration conf, Schema schema, Tablet[] tablets)
      throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Schema getSchema() {
    return meta.getSchema();
  }
}
