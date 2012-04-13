package nta.catalog;

import java.util.Collection;

import org.apache.hadoop.fs.Path;

import nta.catalog.proto.CatalogProtos.ColumnProto;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.SchemaProto;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.catalog.statistics.TableStat;
import nta.engine.query.exception.InvalidQueryException;

public class TCatUtil {
  public static String getCanonicalName(String signature,
      Collection<DataType> paramTypes) {
    DataType [] types = paramTypes.toArray(
        new DataType[paramTypes.size()]);
    return getCanonicalName(signature, types);
  }
  public static String getCanonicalName(String signature,
      DataType...paramTypes) {
    StringBuilder sb = new StringBuilder(signature);
    sb.append("(");
    int i = 0;
    for (DataType type : paramTypes) {
      sb.append(type);
      if(i < paramTypes.length - 1) {
        sb.append(",");
      }
      
      i++;
    }
    sb.append(")");
    return sb.toString();
  }
  
  public static char getTypeCode(DataType type) {
    switch(type) {
    case BOOLEAN: return 'Z';
    case BYTE: return 'B';
    case SHORT: return 'S';
    case INT: return 'I';
    case LONG: return 'J';
    case FLOAT: return 'F';
    case DOUBLE: return 'D';
    case BYTES: return 'N';
    case IPv4: return '4';
    case IPv6: return '6';
    default: throw new InvalidQueryException("Unsupported type exception");
    }
  }

    /**
   * This method transforms the unqualified names of a given schema into
   * the qualified names.
   * 
   * @param tableName a table name to be prefixed
   * @param schema a schema to be transformed
   * 
   * @return
   */

  public static SchemaProto getQualfiedSchema(String tableName,
      SchemaProto schema) {
    SchemaProto.Builder revisedSchema = SchemaProto.newBuilder(schema);
    revisedSchema.clearFields();
    String[] split = null;
    for (ColumnProto col : schema.getFieldsList()) {
      split = col.getColumnName().split("\\.");
      if (split.length == 1) { // if not qualified name
        // rewrite the column
        ColumnProto.Builder builder = ColumnProto.newBuilder(col);
        builder.setColumnName(tableName + "." + col.getColumnName());
        col = builder.build();
      } else if (split.length == 2) {
        ColumnProto.Builder builder = ColumnProto.newBuilder(col);
        builder.setColumnName(tableName + "." + split[1]);
        col = builder.build();
      } else {
        throw new InvalidQueryException("Unaccetable field name "
            + col.getColumnName());
      }
      revisedSchema.addFields(col);
    }

    return revisedSchema.build();
  }
  
  public static TableMeta newTableMeta(Schema schema, StoreType type) {
    return new TableMetaImpl(schema, type, new Options());
  }
  
  public static TableMeta newTableMeta(Schema schema, StoreType type, 
      Options options) {
    return new TableMetaImpl(schema, type, options);
  }
  
  public static TableMeta newTableMeta(Schema schema, StoreType type, Options options, 
      TableStat stat) {
    return new TableMetaImpl(schema, type, options, stat);
  }
  
  public static TableDesc newTableDesc(String tableName, TableMeta meta, 
      Path path) {
    return new TableDescImpl(tableName, meta, path);
  }
  
  public static TableDesc newTableDesc(TableDescProto proto) {
    return new TableDescImpl(proto);
  }
  
  public static TableDesc newTableDesc(String tableName, 
      Schema schema, StoreType type, Options options, Path path) {
    return new TableDescImpl(tableName, schema, type, options, path);
  }
}