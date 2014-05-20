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

package org.apache.tajo.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;
import parquet.hadoop.ParquetOutputFormat;

import java.io.IOException;

public class StorageUtil extends StorageConstants{
  public static int getRowByteSize(Schema schema) {
    int sum = 0;
    for(Column col : schema.getColumns()) {
      sum += StorageUtil.getColByteSize(col);
    }

    return sum;
  }

  public static int getColByteSize(Column col) {
    switch (col.getDataType().getType()) {
      case BOOLEAN:
        return 1;
      case CHAR:
        return 1;
      case BIT:
        return 1;
      case INT2:
        return 2;
      case INT4:
        return 4;
      case INT8:
        return 8;
      case FLOAT4:
        return 4;
      case FLOAT8:
        return 8;
      case INET4:
        return 4;
      case INET6:
        return 32;
      case TEXT:
        return 256;
      case BLOB:
        return 256;
      case DATE:
        return 4;
      case TIME:
        return 8;
      case TIMESTAMP:
        return 8;
      default:
        return 0;
    }
  }

  public static void writeTableMeta(Configuration conf, Path tableroot, TableMeta meta) throws IOException {
    FileSystem fs = tableroot.getFileSystem(conf);
    FSDataOutputStream out = fs.create(new Path(tableroot, ".meta"));
    FileUtil.writeProto(out, meta.getProto());
    out.flush();
    out.close();
  }
  
  public static Path concatPath(String parent, String...childs) {
    return concatPath(new Path(parent), childs);
  }
  
  public static Path concatPath(Path parent, String...childs) {
    StringBuilder sb = new StringBuilder();
    
    for(int i=0; i < childs.length; i++) {      
      sb.append(childs[i]);
      if(i < childs.length - 1)
        sb.append("/");
    }
    
    return new Path(parent, sb.toString());
  }

  public static KeyValueSet newPhysicalProperties(CatalogProtos.StoreType type) {
    KeyValueSet options = new KeyValueSet();
    if (CatalogProtos.StoreType.CSV == type) {
      options.put(CSVFILE_DELIMITER, DEFAULT_FIELD_DELIMITER);
    } else if (CatalogProtos.StoreType.RCFILE == type) {
      options.put(RCFILE_SERDE, DEFAULT_BINARY_SERDE);
    } else if (CatalogProtos.StoreType.SEQUENCEFILE == type) {
      options.put(SEQUENCEFILE_SERDE, DEFAULT_TEXT_SERDE);
      options.put(SEQUENCEFILE_DELIMITER, DEFAULT_FIELD_DELIMITER);
    } else if (type == CatalogProtos.StoreType.PARQUET) {
      options.put(ParquetOutputFormat.BLOCK_SIZE, PARQUET_DEFAULT_BLOCK_SIZE);
      options.put(ParquetOutputFormat.PAGE_SIZE, PARQUET_DEFAULT_PAGE_SIZE);
      options.put(ParquetOutputFormat.COMPRESSION, PARQUET_DEFAULT_COMPRESSION_CODEC_NAME);
      options.put(ParquetOutputFormat.ENABLE_DICTIONARY, PARQUET_DEFAULT_IS_DICTIONARY_ENABLED);
      options.put(ParquetOutputFormat.VALIDATION, PARQUET_DEFAULT_IS_VALIDATION_ENABLED);
    }

    return options;
  }
}
