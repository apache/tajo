/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.catalog;

import tajo.catalog.proto.CatalogProtos;
import tajo.catalog.proto.CatalogProtos.ColumnProto;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.SchemaProto;
import tajo.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static tajo.catalog.proto.CatalogProtos.StoreType;

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
    default: throw new InternalError("Unsupported type exception");
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
        throw new InternalError("Unaccetable field name "
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

  public static StoreType getStoreType(final String typeStr) {
    if (typeStr.equalsIgnoreCase(StoreType.CSV.name())) {
      return StoreType.CSV;
    } else if (typeStr.equalsIgnoreCase(StoreType.RAW.name())) {
      return StoreType.RAW;
    } else if (typeStr.equalsIgnoreCase(StoreType.CSV.name())) {
      return StoreType.CSV;
    } else if (typeStr.equalsIgnoreCase(StoreType.ROWFILE.name())) {
      return StoreType.ROWFILE;
    }else if (typeStr.equalsIgnoreCase(StoreType.RCFILE.name())) {
      return StoreType.RCFILE;
    } else if (typeStr.equalsIgnoreCase(StoreType.TREVNI.name())) {
      return StoreType.TREVNI;
    } else {
      return null;
    }
  }
}
