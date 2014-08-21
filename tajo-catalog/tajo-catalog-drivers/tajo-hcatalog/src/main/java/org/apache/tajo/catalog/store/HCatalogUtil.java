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
package org.apache.tajo.catalog.store;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.tajo.catalog.exception.CatalogException;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.thrift.TException;
import parquet.hadoop.mapred.DeprecatedParquetOutputFormat;

public class HCatalogUtil {
  protected final Log LOG = LogFactory.getLog(getClass());

  public static void validateHCatTableAndTajoSchema(HCatSchema tblSchema) throws CatalogException {
    for (HCatFieldSchema hcatField : tblSchema.getFields()) {
      validateHCatFieldAndTajoSchema(hcatField);
    }
  }

  private static void validateHCatFieldAndTajoSchema(HCatFieldSchema fieldSchema) throws CatalogException {
    try {
      HCatFieldSchema.Type fieldType = fieldSchema.getType();
      switch (fieldType) {
        case ARRAY:
          throw new HCatException("Tajo cannot support array field type.");
        case STRUCT:
          throw new HCatException("Tajo cannot support struct field type.");
        case MAP:
          throw new HCatException("Tajo cannot support map field type.");
      }
    } catch (HCatException e) {
      throw new CatalogException("incompatible hcatalog types when assigning to tajo type. - " +
          "HCatFieldSchema:" + fieldSchema);
    }
  }

  public static TajoDataTypes.Type getTajoFieldType(String fieldType)  {
    Preconditions.checkNotNull(fieldType);

    String typeStr = null;

    if(fieldType.equalsIgnoreCase(serdeConstants.INT_TYPE_NAME))
      typeStr = "INT4";
    else if(fieldType.equalsIgnoreCase(serdeConstants.TINYINT_TYPE_NAME))
      typeStr = "INT1";
    else if(fieldType.equalsIgnoreCase(serdeConstants.SMALLINT_TYPE_NAME))
      typeStr = "INT2";
    else if(fieldType.equalsIgnoreCase(serdeConstants.BIGINT_TYPE_NAME))
      typeStr = "INT8";
    else if(fieldType.equalsIgnoreCase(serdeConstants.BOOLEAN_TYPE_NAME))
      typeStr = "BOOLEAN";
    else if(fieldType.equalsIgnoreCase(serdeConstants.FLOAT_TYPE_NAME))
      typeStr = "FLOAT4";
    else if(fieldType.equalsIgnoreCase(serdeConstants.DOUBLE_TYPE_NAME))
      typeStr = "FLOAT8";
    else if(fieldType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME))
      typeStr = "TEXT";
    else if(fieldType.equalsIgnoreCase(serdeConstants.BINARY_TYPE_NAME))
      typeStr = "BLOB";

    try {
      return Enum.valueOf(TajoDataTypes.Type.class, typeStr);
    } catch (IllegalArgumentException iae) {
      throw new CatalogException("Cannot find a matched type aginst from '" + typeStr + "'");
    }
  }

  public static String getHiveFieldType(TajoDataTypes.DataType dataType) {
    Preconditions.checkNotNull(dataType);

    switch (dataType.getType()) {
    case CHAR: return serdeConstants.CHAR_TYPE_NAME;
    case BOOLEAN: return serdeConstants.BOOLEAN_TYPE_NAME;
    case INT1: return serdeConstants.TINYINT_TYPE_NAME;
    case INT2: return serdeConstants.SMALLINT_TYPE_NAME;
    case INT4: return serdeConstants.INT_TYPE_NAME;
    case INT8: return serdeConstants.BIGINT_TYPE_NAME;
    case FLOAT4: return serdeConstants.FLOAT_TYPE_NAME;
    case FLOAT8: return serdeConstants.DOUBLE_TYPE_NAME;
    case TEXT: return serdeConstants.STRING_TYPE_NAME;
    case VARCHAR: return serdeConstants.VARCHAR_TYPE_NAME;
    case NCHAR: return serdeConstants.VARCHAR_TYPE_NAME;
    case NVARCHAR: return serdeConstants.VARCHAR_TYPE_NAME;
    case BINARY: return serdeConstants.BINARY_TYPE_NAME;
    case VARBINARY: return serdeConstants.BINARY_TYPE_NAME;
    case BLOB: return serdeConstants.BINARY_TYPE_NAME;
    case DATE: return serdeConstants.DATE_TYPE_NAME;
    case TIMESTAMP: return serdeConstants.TIMESTAMP_TYPE_NAME;
    default:
      throw new CatalogException(dataType + " is not supported.");
    }
  }

  public static String getStoreType(String fileFormat) {
    Preconditions.checkNotNull(fileFormat);

    String[] fileFormatArrary = fileFormat.split("\\.");
    if(fileFormatArrary.length < 1) {
      throw new CatalogException("Hive file output format is wrong. - file output format:" + fileFormat);
    }

    String outputFormatClass = fileFormatArrary[fileFormatArrary.length-1];
    if(outputFormatClass.equals(HiveIgnoreKeyTextOutputFormat.class.getSimpleName())) {
      return CatalogProtos.StoreType.CSV.name();
    } else if(outputFormatClass.equals(HiveSequenceFileOutputFormat.class.getSimpleName())) {
      return CatalogProtos.StoreType.SEQUENCEFILE.name();
    } else if(outputFormatClass.equals(RCFileOutputFormat.class.getSimpleName())) {
      return CatalogProtos.StoreType.RCFILE.name();
    } else if(outputFormatClass.equals(DeprecatedParquetOutputFormat.class.getSimpleName())) {
      return CatalogProtos.StoreType.PARQUET.name();
    } else {
      throw new CatalogException("Not supported file output format. - file output format:" + fileFormat);
    }
  }

  public static Table getTable(IMetaStoreClient client, String dbName, String tableName) throws TException {
    return new Table(client.getTable(dbName, tableName));
  }
}
