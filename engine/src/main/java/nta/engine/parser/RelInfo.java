package nta.engine.parser;

import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.engine.SchemaObject;

public class RelInfo implements SchemaObject {
	public String alias;
	public TableDesc rel;
	
	public RelInfo(TableDesc rel) {
		this.rel = rel;
	}
	
	public RelInfo(TableDesc rel, String alias) {
		this(rel);
		this.alias = alias;
	}
	
	public void setTableDesc(TableDesc rel) {
		this.rel = rel;
	}
	
	public boolean hasAlias() {
		return alias != null;
	}
	
	public Schema getSchema() {
		return this.rel.getInfo().getSchema();
	}
	
	public TableDesc getRelation() {
		return this.rel;
	}
	
	public String alias() {
		return this.alias;
	}
	
	public String getName() {
		return rel.getName();
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.rel);			
		if(hasAlias()) {
			sb.append("("+alias+")");
		}
		return sb.toString();
	}
}
