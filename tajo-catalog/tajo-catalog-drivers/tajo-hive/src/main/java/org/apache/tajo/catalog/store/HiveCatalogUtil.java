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
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.LMDNoMatchedDatatypeException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnknownDataFormatException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.type.Type;
import org.apache.tajo.type.TypeStringEncoder;
import org.apache.thrift.TException;

public class HiveCatalogUtil {
  public static void validateSchema(Table tblSchema) {
    for (FieldSchema fieldSchema : tblSchema.getCols()) {
      String fieldType = fieldSchema.getType();
      if (fieldType.equalsIgnoreCase("ARRAY") || fieldType.equalsIgnoreCase("STRUCT")
        || fieldType.equalsIgnoreCase("MAP")) {
        throw new TajoRuntimeException(new UnsupportedException("data type '" + fieldType.toUpperCase() + "'"));
      }
    }
  }

  public static TajoDataTypes.Type getTajoFieldType(String dataType) throws LMDNoMatchedDatatypeException {
    Preconditions.checkNotNull(dataType);

    if(dataType.equalsIgnoreCase(serdeConstants.INT_TYPE_NAME)) {
      return TajoDataTypes.Type.INT4;
    } else if(dataType.equalsIgnoreCase(serdeConstants.TINYINT_TYPE_NAME)) {
      return TajoDataTypes.Type.INT1;
    } else if(dataType.equalsIgnoreCase(serdeConstants.SMALLINT_TYPE_NAME)) {
      return TajoDataTypes.Type.INT2;
    } else if(dataType.equalsIgnoreCase(serdeConstants.BIGINT_TYPE_NAME)) {
      return TajoDataTypes.Type.INT8;
    } else if(dataType.equalsIgnoreCase(serdeConstants.BOOLEAN_TYPE_NAME)) {
      return TajoDataTypes.Type.BOOLEAN;
    } else if(dataType.equalsIgnoreCase(serdeConstants.FLOAT_TYPE_NAME)) {
      return TajoDataTypes.Type.FLOAT4;
    } else if(dataType.equalsIgnoreCase(serdeConstants.DOUBLE_TYPE_NAME)) {
      return TajoDataTypes.Type.FLOAT8;
    } else if(dataType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME)) {
      return TajoDataTypes.Type.TEXT;
    } else if(dataType.equalsIgnoreCase(serdeConstants.BINARY_TYPE_NAME)) {
      return TajoDataTypes.Type.BLOB;
    } else if(dataType.equalsIgnoreCase(serdeConstants.TIMESTAMP_TYPE_NAME)) {
      return TajoDataTypes.Type.TIMESTAMP;
    } else if(dataType.equalsIgnoreCase(serdeConstants.DATE_TYPE_NAME)) {
      return TajoDataTypes.Type.DATE;
    } else {
      throw new LMDNoMatchedDatatypeException(dataType);
    }
  }

  public static String getHiveFieldType(Type type) throws LMDNoMatchedDatatypeException {
    Preconditions.checkNotNull(type);

    switch (type.kind()) {
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
      throw new LMDNoMatchedDatatypeException(TypeStringEncoder.encode(type));
    }
  }

  public static String getDataFormat(StorageDescriptor descriptor) {
    Preconditions.checkNotNull(descriptor);

    String serde = descriptor.getSerdeInfo().getSerializationLib();
    String inputFormat = descriptor.getInputFormat();

    if (LazySimpleSerDe.class.getName().equals(serde)) {
      if (TextInputFormat.class.getName().equals(inputFormat)) {
        return BuiltinStorages.TEXT;
      } else if (SequenceFileInputFormat.class.getName().equals(inputFormat)) {
        return BuiltinStorages.SEQUENCE_FILE;
      } else {
        throw new TajoRuntimeException(new UnknownDataFormatException(inputFormat));
      }
    } else if (LazyBinarySerDe.class.getName().equals(serde)) {
      if (SequenceFileInputFormat.class.getName().equals(inputFormat)) {
        return BuiltinStorages.SEQUENCE_FILE;
      } else {
        throw new TajoRuntimeException(new UnknownDataFormatException(inputFormat));
      }
    } else if (LazyBinaryColumnarSerDe.class.getName().equals(serde) || ColumnarSerDe.class.getName().equals(serde)) {
      if (RCFileInputFormat.class.getName().equals(inputFormat)) {
        return BuiltinStorages.RCFILE;
      } else {
        throw new TajoRuntimeException(new UnknownDataFormatException(inputFormat));
      }
    } else if (ParquetHiveSerDe.class.getName().equals(serde)) {
      return BuiltinStorages.PARQUET;
    } else if (AvroSerDe.class.getName().equals(serde)) {
      return BuiltinStorages.AVRO;
    } else if (OrcSerde.class.getName().equals(serde)) {
      return BuiltinStorages.ORC;
    } else {
      throw new TajoRuntimeException(new UnknownDataFormatException(inputFormat));
    }
  }

  public static Table getTable(IMetaStoreClient client, String dbName, String tableName) throws TException {
    return new Table(client.getTable(dbName, tableName));
  }
}
