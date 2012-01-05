package nta.engine.parser;

import nta.catalog.Schema;
import nta.catalog.TableInfo;
import nta.engine.SchemaObject;

public class RelInfo implements SchemaObject {
	public String alias;
	public TableInfo rel;
	
	public RelInfo(TableInfo rel) {
		this.rel = rel;
	}
	
	public RelInfo(TableInfo rel, String alias) {
		this(rel);
		this.alias = alias;
	}
	
	public void setTableDesc(TableInfo rel) {
		this.rel = rel;
	}
	
	public boolean hasAlias() {
		return alias != null;
	}
	
	public Schema getSchema() {
		return this.rel.getSchema();
	}
	
	public TableInfo getRelation() {
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
