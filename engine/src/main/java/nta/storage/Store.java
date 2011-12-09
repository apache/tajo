package nta.storage;

import java.net.URI;

import nta.catalog.Schema;
import nta.catalog.TableInfo;
import nta.catalog.proto.TableProtos.StoreType;

public class Store {
	private final URI uri;
	private final TableInfo tableBase;
	
	Store(URI uri, TableInfo table) {			
		this.uri = uri;
		this.tableBase = table;
	}
	
	public URI getURI() {
		return this.uri;
	}
	
	public TableInfo getTableBase() {
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
}
