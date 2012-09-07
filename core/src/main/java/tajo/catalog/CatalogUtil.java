package tajo.catalog;

import org.apache.hadoop.fs.Path;
import tajo.catalog.proto.CatalogProtos;
import tajo.catalog.proto.CatalogProtos.ColumnProto;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.SchemaProto;
import tajo.engine.query.exception.InvalidQueryException;
import tajo.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class CatalogUtil {
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
    String[] split;
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

  public static String prettyPrint(TableMeta meta) {
    StringBuilder sb = new StringBuilder();
    sb.append("store type: ").append(meta.getStoreType()).append("\n");
    sb.append("schema: \n");

    for(int i = 0; i < meta.getSchema().getColumnNum(); i++) {
      Column col = meta.getSchema().getColumn(i);
      sb.append(col.getColumnName()).append("\t").append(col.getDataType());
      sb.append("\n");
    }
    return sb.toString();
  }

  public static void printTableMeta(File file) throws IOException {
    CatalogProtos.TableProto proto = (CatalogProtos.TableProto) FileUtil.
        loadProto(file, CatalogProtos.TableProto.getDefaultInstance());
    System.out.println(prettyPrint(new TableMetaImpl(proto)));
  }

  public static void main(String [] args) throws IOException {
    if (args.length < 2) {
      System.out.println("catalog print [filename]");
      System.exit(-1);
    }

    File file = new File(args[1]);
    printTableMeta(file);
  }
}
