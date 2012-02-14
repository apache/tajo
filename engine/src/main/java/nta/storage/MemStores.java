/**
 * 
 */
package nta.storage;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.datum.exception.InvalidCastException;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

/**
 * @author hyunsik
 *
 */
public class MemStores {
	private Configuration conf;
	
	private Map<URI, MemTable> slots = Maps.newHashMap();
	
	public MemStores(Configuration conf) {
		this.conf = conf;
	}
	
	public MemTable getMemStore(URI uri) {
		return slots.get(uri);
	}
	
	public void addMemStore(URI uri, MemTable slot) {
		if(slots.containsKey(uri)) {
			
		}		
		slots.put(uri, slot);
	}
	
	public void dropMemStore(URI uri) {
		slots.remove(uri);
	}
	
	public class MemTableAppender extends FileAppender {
	  MemTable table;
	  public MemTableAppender(Configuration conf, MemTable table, 
	      TableMeta meta) {
	    super(conf, meta, null);
	  }

	  @Override
	  public void addTuple(Tuple tuple) throws IOException {
	    table.slots.add((VTuple) tuple);
	  }

    @Override
    public void flush() throws IOException {
      
    }

    @Override
    public void close() throws IOException {
      
    }
	}
	
	public class MemTableScanner implements Scanner {
	  Schema schema;
	  private int cur = -1;
    MemTable table;
    
	  public MemTableScanner(Schema schema, MemTable table) {
      this.schema = schema;
      this.table = table;
    }

    public Tuple next() throws IOException {
      cur++;    
      if(cur < slots.size()) {
        Tuple t = table.slots.get(cur);
        VTuple tuple = new VTuple(schema.getColumnNum());
        
        Column field = null;
        for(int i=0; i < schema.getColumnNum(); i++) {
          field = schema.getColumn(i);

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
            throw new InvalidCastException("IPv6 is unsupported yet");
            
          default:
            ;
          }       
        }     
        
        return tuple;
      } else
        return null;
    }
    
    public void reset() {
      this.cur = -1;
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void close() throws IOException {
      this.table = null;      
    }
	}
}
