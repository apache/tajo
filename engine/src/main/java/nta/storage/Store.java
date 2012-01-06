package nta.storage;

import java.net.URI;

import nta.catalog.Schema;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.TableProtos.StoreType;

public class Store {
	private final URI uri;
	private final TableMetaImpl tableBase;
	
	Store(URI uri, TableMetaImpl table) {			
		this.uri = uri;
		this.tableBase = table;
	}
	
	public URI getURI() {
		return this.uri;
	}
	
	public TableMetaImpl getTableBase() {
		return this.tableBase;
	}
	
	public Schema getSchema() {
		return tableBase.getSchema();
	}
	
	public StoreType getStoreType() {
		return tableBase.getStoreType();
	}
	
	public String getOption(String key) {
		return tableBase.getOption(key);
	}
	
	public boolean equals(Object obj) {
	  Store other = (Store) obj;
	  
	  return this.uri.equals(other.uri) &&
	      this.tableBase.equals(other.tableBase);
	}
}
