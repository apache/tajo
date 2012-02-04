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
}
