package nta.catalog;

import nta.catalog.proto.CatalogProtos;
import nta.engine.parser.QueryBlock;
import nta.catalog.proto.CatalogProtos.DataType;

import java.util.Arrays;
import java.util.Collection;

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

  public static Schema merge(QueryBlock.FromTable [] fromTables) {
    Schema merged = new Schema();
    for (QueryBlock.FromTable table : fromTables) {
      merged.addColumns(table.getSchema());
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

  public static Schema mergeAllWithNoDup(Collection<Column>...columnList) {
    Schema merged = new Schema();
    for (Collection<Column> columns : columnList) {
      for (Column col : columns) {
        if (merged.contains(col.getQualifiedName())) {
          continue;
        }
        merged.addColumn(col);
      }
    }

    return merged;
  }

  public static Schema getProjectedSchema(Schema inSchema, Collection<Column> columns) {
    Schema projected = new Schema();
    for (Column col : columns) {
      if (inSchema.contains(col.getQualifiedName())) {
        projected.addColumn(col);
      }
    }

    return projected;
  }

  public static CatalogProtos.DataType[] newNoNameSchema(CatalogProtos.DataType... types) {
    DataType [] dataTypes = types.clone();
    return dataTypes;
  }
}
