/**
 * 
 */
package nta.catalog;

import org.antlr.runtime.tree.Tree;

/**
 * @author Hyunsik Choi
 *
 */
public class FieldName {
	private final String tableName;
	private final String familyName;
	private final String fieldName;
	
	public FieldName(Tree tree) {
		if(tree.getChild(1) == null) {
			this.tableName = null;
		} else {
			this.tableName = tree.getChild(1).getText();
		}
		
		String name = tree.getChild(0).getText();
		if(name.contains(":")) {
			String [] splits = name.split(":");
			this.familyName = splits[0];
			this.fieldName = splits[1];
		} else {
			this.familyName = null;
			this.fieldName = name;
		}
	}
	
	public boolean hasTableName() {
		return this.tableName != null;
	}
	
	public String getTableName(){
		return this.tableName;
	}
	
	public boolean hasFamilyName() {
		return this.familyName != null;
	}
	
	public String getFamilyName() {
		return this.familyName;
	}
	
	public String getFullName() {
		StringBuilder sb = new StringBuilder();
		if(tableName != null) {
			sb.append(tableName)
			.append(".");
		}
		if(familyName != null) {
			sb.append(familyName).
			append(":");
		}
		sb.append(fieldName);
		
		return sb.toString();
	}
	
	public String getName() {
		if(familyName == null) {
			return fieldName;
		} else 
			return familyName+":"+fieldName;
	}
	
	public String getSimpleName() {
		return this.fieldName;
	}
	
	public String toString() {
		return getName();
	}
}
