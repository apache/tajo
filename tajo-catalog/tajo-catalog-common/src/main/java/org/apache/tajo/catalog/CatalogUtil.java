/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.catalog;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableDescProto;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import static org.apache.tajo.common.TajoDataTypes.Type;

public class CatalogUtil {
  public static String getCanonicalName(String signature,
      Collection<DataType> paramTypes) {
    DataType [] types = paramTypes.toArray(new DataType[paramTypes.size()]);
    return getCanonicalName(signature, types);
  }
  public static String getCanonicalName(String signature,
      DataType...paramTypes) {
    StringBuilder sb = new StringBuilder(signature);
    sb.append("(");
    int i = 0;
    for (DataType type : paramTypes) {
      sb.append(type.getType());
      if(i < paramTypes.length - 1) {
        sb.append(",");
      }
      
      i++;
    }
    sb.append(")");
    return sb.toString();
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

  public static DataType newDataTypeWithoutLen(Type type) {
    return DataType.newBuilder().setType(type).build();
  }

  public static DataType [] newDataTypesWithoutLen(Type... types) {
    DataType [] dataTypes = new DataType[types.length];
    for (int i = 0; i < types.length; i++) {
      dataTypes[i] = DataType.newBuilder().setType(types[i]).build();
    }
    return dataTypes;
  }

  public static DataType newDataTypeWithLen(Type type, int length) {
    return DataType.newBuilder().setType(type).setLength(length).build();
  }
}
