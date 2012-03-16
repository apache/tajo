package nta.catalog;



public class SchemaUtil {
  public static Schema merge(Schema left, Schema right) {
    Schema merged = new Schema();
    for(Column col : left.getColumns()) {
      merged.addColumn(col);
    }
    for(Column col : right.getColumns()) {
      merged.addColumn(col);
    }
    
    return merged;
  }
  
  public static Schema getCommons(Schema left, Schema right) {
    Schema common = new Schema();
    for (Column outer : left.getColumns()) {
      for (Column inner : right.getColumns()) {
        if (outer.getColumnName().equals(inner.getColumnName()) &&
            outer.getDataType() == inner.getDataType()) {
          common.addColumn(outer.getColumnName(), outer.getDataType());
        }
      }
    }
    
    return common;
  }
}
