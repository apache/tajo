package nta.engine.exec.eval;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.SchemaUtil;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.engine.json.GsonCreator;
import nta.storage.Tuple;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi 
 */
public class FieldEval extends EvalNode implements Cloneable {
	@Expose private Column column;
	@Expose	private int fieldId = -1;
  private Datum datum;
	
	public FieldEval(String columnName, DataType domain) {
		super(Type.FIELD);
		this.column = new Column(columnName, domain);
	}
	
	public FieldEval(Column column) {
	  super(Type.FIELD);
	  this.column = column;
	}

	@Override
	public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
	  if (fieldId == -1) {
	    fieldId = schema.getColumnId(column.getQualifiedName());
	  }
	  datum = tuple.get(fieldId);
	}

  @Override
  public Datum terminate(EvalContext ctx) {
    return this.datum;
  }

  @Override
  public EvalContext newContext() {
    return null;
  }

  @Override
	public DataType [] getValueType() {
		return SchemaUtil.newNoNameSchema(column.getDataType());
	}
	
  public Column getColumnRef() {
    return column;
  }
	
	public String getTableId() {	  
	  return column.getTableName();
	}
	
	public String getColumnName() {
	  return column.getColumnName();
	}
	
	public void replaceColumnRef(String columnName) {
	  this.column.setName(columnName);
	}

	@Override
	public String getName() {
		return this.column.getQualifiedName();
	}
	
	public String toString() {
	  return this.column.toString();
	}
	
  public boolean equals(Object obj) {
    if (obj instanceof FieldEval) {
      FieldEval other = (FieldEval) obj;
      
      return column.equals(other.column);      
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return column.hashCode();
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    FieldEval eval = (FieldEval) super.clone();
    eval.column = (Column) this.column.clone();
    eval.fieldId = fieldId;
    
    return eval;
  }
  
  public String toJSON() {
    Gson gson = GsonCreator.getInstance();
    return gson.toJson(this, EvalNode.class);
  }

  public void preOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
  }
  
  @Override
  public void postOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
  }
}