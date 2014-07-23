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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;
import parquet.hadoop.ParquetOutputFormat;
import sun.nio.ch.DirectBuffer;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class StorageUtil extends StorageConstants {
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

  static final String fileNamePatternV08 = "part-[0-9]*-[0-9]*";
  static final String fileNamePatternV09 = "part-[0-9]*-[0-9]*-[0-9]*";

  /**
   * Written files can be one of two forms: "part-[0-9]*-[0-9]*" or "part-[0-9]*-[0-9]*-[0-9]*".
   *
   * This method finds the maximum sequence number from existing data files through the above patterns.
   * If it cannot find any matched file or the maximum number, it will return -1.
   *
   * @param fs
   * @param path
   * @param recursive
   * @return The maximum sequence number
   * @throws IOException
   */
  public static int getMaxFileSequence(FileSystem fs, Path path, boolean recursive) throws IOException {
    if (!fs.isDirectory(path)) {
      return -1;
    }

    FileStatus[] files = fs.listStatus(path);

    if (files == null || files.length == 0) {
      return -1;
    }

    int maxValue = -1;
    List<Path> fileNamePatternMatchedList = new ArrayList<Path>();

    for (FileStatus eachFile: files) {
      // In the case of partition table, return largest value within all partition dirs.
      if (eachFile.isDirectory() && recursive) {
        int value = getMaxFileSequence(fs, eachFile.getPath(), recursive);
        if (value > maxValue) {
          maxValue = value;
        }
      } else {
        if (eachFile.getPath().getName().matches(fileNamePatternV08) ||
            eachFile.getPath().getName().matches(fileNamePatternV09)) {
          fileNamePatternMatchedList.add(eachFile.getPath());
        }
      }
    }

    if (fileNamePatternMatchedList.isEmpty()) {
      return maxValue;
    }
    Path lastFile = fileNamePatternMatchedList.get(fileNamePatternMatchedList.size() - 1);
    String pathName = lastFile.getName();

    // 0.8: pathName = part-<ExecutionBlockId.seq>-<QueryUnitId.seq>
    // 0.9: pathName = part-<ExecutionBlockId.seq>-<QueryUnitId.seq>-<Sequence>
    String[] pathTokens = pathName.split("-");
    if (pathTokens.length == 3) {
      return -1;
    } else if(pathTokens.length == 4) {
      return Integer.parseInt(pathTokens[3]);
    } else {
      return -1;
    }
  }

  public static void closeBuffer(ByteBuffer buffer) {
    if (buffer != null) {
      if (buffer.isDirect()) {
        ((DirectBuffer) buffer).cleaner().clean();
      } else {
        buffer.clear();
      }
    }
  }

  public static int readFully(InputStream is, byte[] buffer, int offset, int length)
      throws IOException {
    int nread = 0;
    while (nread < length) {
      int nbytes = is.read(buffer, offset + nread, length - nread);
      if (nbytes < 0) {
        return nread > 0 ? nread : nbytes;
      }
      nread += nbytes;
    }
    return nread;
  }

  /**
   * Similar to readFully(). Skips bytes in a loop.
   * @param in The DataInput to skip bytes from
   * @param len number of bytes to skip.
   * @throws java.io.IOException if it could not skip requested number of bytes
   * for any reason (including EOF)
   */
  public static void skipFully(DataInput in, int len) throws IOException {
    int amt = len;
    while (amt > 0) {
      long ret = in.skipBytes(amt);
      if (ret == 0) {
        // skip may return 0 even if we're not at EOF.  Luckily, we can
        // use the read() method to figure out if we're at the end.
        int b = in.readByte();
        if (b == -1) {
          throw new EOFException( "Premature EOF from inputStream after " +
              "skipping " + (len - amt) + " byte(s).");
        }
        ret = 1;
      }
      amt -= ret;
    }
  }
}
