/**
 * 
 */
package nta.catalog;

import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableProto;
import nta.catalog.proto.TableProtos.TableType;
import nta.storage.Store;

/**
 * @author hyunsik
 *
 */
public class TableMeta extends TableInfo {

	public TableMeta() {
		super();
	}
	
	public TableMeta(String name) {
		this.name = name;
	}
	
	public TableMeta(String name, Store store) {
		super(store.getTableBase().getProto());
		this.name = name;
		this.store = store;
	}

	/**
	 * @param proto
	 */
	public TableMeta(TableProto proto) {
		super(proto);
	}
	
	public void setId(int id) {
		this.tableId = id;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public void setStore(Store store) {
		this.store = store;
	}
	
	public void setStorageType(StoreType storeType) {
		maybeInitBuilder();
		this.storeType = storeType;
	}
	
	public void setTableType(TableType tableType) {
		maybeInitBuilder();
		this.tableType = tableType;
	}
	
	public void setSchema(Schema schema) {
		maybeInitBuilder();
		this.schema = schema;
	}
	
	public void setStartKey(long startKey) {
		maybeInitBuilder();
		this.startKey = startKey;
	}
	
	public void setEndKey(long endKey) {
		maybeInitBuilder();
		this.endKey = endKey;
	}	
	
	public void setDuration(long duration) {
		maybeInitBuilder();
		this.duration = duration;
	}
	
	public void setOptions(Options options) {
		maybeInitBuilder();
		this.options = options;
	}
	
	public void putOption(String key, String val) {
		initOptions();
		this.options.put(key, val);
	}
	
	public String delete(String key) {
		initOptions();
		return this.options.delete(key);
	}
	
	public boolean equals(Object object) {
    if(object instanceof TableMeta) {
      TableMeta other = (TableMeta) object;
      
      if(this.tableId != other.tableId) {
        return false;
      }
      if(this.name != null && other.name != null) {
        if(!this.name.equals(other.name))
          return false; 
      } else if(this.name == null ^ other.name == null) {
        return false;
      }
      
      if(this.store != null && other.store != null) {
        if(!this.store.equals(other.store))
          return false; 
      } else if(this.store == null ^ other.store == null) {
        return false;
      }
      
      //return true;
      return this.getProto().equals(other.getProto());       
    }
    
    return false;   
  }
	
	public Object clone() {
	  return super.clone();
	}
	
	public String toString() {
	  StringBuilder str = new StringBuilder();
	  str.append("{")
	  .append("tableId: "+this.tableId).append("\n")
	  .append("name: "+this.name).append("\n")
	  .append("store: "+this.store).append("\n")
	  .append("proto: "+this.proto.toString()).append("\n}");
	  
	  return str.toString();
	}
}
