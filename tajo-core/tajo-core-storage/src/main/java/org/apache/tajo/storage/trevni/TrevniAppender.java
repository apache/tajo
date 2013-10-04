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

package org.apache.tajo.storage.trevni;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.storage.FileAppender;
import org.apache.tajo.storage.TableStatistics;
import org.apache.tajo.storage.Tuple;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.ColumnFileWriter;
import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ValueType;

import java.io.FileNotFoundException;
import java.io.IOException;

public class TrevniAppender extends FileAppender {
  private FileSystem fs;
  private ColumnFileWriter writer;
  private FSDataOutputStream fos;

  private TableStatistics stats = null;
  private boolean flushed = false;

  public TrevniAppender(Configuration conf, TableMeta meta, Path path) throws IOException {
    super(conf, meta, path);
  }

  public void init() throws IOException {
    fs = path.getFileSystem(conf);

    if (!fs.exists(path.getParent())) {
      throw new FileNotFoundException(path.toString());
    }

    fos = fs.create(path);

    ColumnMetaData [] trevniMetas =
        new ColumnMetaData[meta.getSchema().getColumnNum()];
    int i = 0;
    for (Column column : meta.getSchema().getColumns()) {
      trevniMetas[i++] = new ColumnMetaData(column.getColumnName(),
          getType(column.getDataType().getType()));
    }

    writer = new ColumnFileWriter(createFileMeta(), trevniMetas);

    if (enabledStats) {
      this.stats = new TableStatistics(this.schema);
    }

    super.init();
  }

  private ColumnFileMetaData createFileMeta() {
    return new ColumnFileMetaData()
        .setCodec("null")
        .setChecksum("null");
  }

  private static ValueType getType(Type type) {
    switch (type) {
      case BOOLEAN:
        return ValueType.INT;
      case BIT:
        return ValueType.INT;
      case CHAR:
        return ValueType.STRING;
      case INT2:
        return ValueType.INT;
      case INT4:
        return ValueType.INT;
      case INT8:
        return ValueType.LONG;
      case FLOAT4:
        return ValueType.FLOAT;
      case FLOAT8:
        return ValueType.DOUBLE;
      case TEXT:
        return ValueType.STRING;
      case BLOB:
        return ValueType.BYTES;
      case INET4:
        return ValueType.BYTES;
      case INET6:
        return ValueType.BYTES;
      case PROTOBUF:
        return ValueType.BYTES;
      case NULL:
        return ValueType.NULL;
      default:
        return null;
    }
  }

  @Override
  public long getOffset() throws IOException {
    return 0;
  }

  @Override
  public void addTuple(Tuple t) throws IOException {
    Column col;
    writer.startRow();
    for (int i = 0; i < schema.getColumnNum(); i++) {
      if (enabledStats) {
        stats.analyzeField(i, t.get(i));
      }

      if (!t.isNull(i)) {
        col = schema.getColumn(i);
        switch (col.getDataType().getType()) {
          case NULL:
            break;
          case BOOLEAN:
          case BIT:
          case INT2:
          case INT4:
            writer.writeValue(t.get(i).asInt4(), i);
            break;
          case INT8:
            writer.writeValue(t.get(i).asInt8(), i);
            break;
          case FLOAT4:
            writer.writeValue(t.get(i).asFloat4(), i);
            break;
          case FLOAT8:
            writer.writeValue(t.get(i).asFloat8(), i);
            break;
          case CHAR:
          case TEXT:
            writer.writeValue(t.get(i).asChars(), i);
            break;
          case PROTOBUF:
          case BLOB:
          case INET4:
          case INET6:
            writer.writeValue(t.get(i).asByteArray(), i);

          default:
            break;
        }
      }
    }
    writer.endRow();

    // Statistical section
    if (enabledStats) {
      stats.incrementRow();
    }
  }

  @Override
  public void flush() throws IOException {
    if (!flushed) {
      writer.writeTo(fos);
      fos.flush();
      flushed = true;
    }
  }

  @Override
  public void close() throws IOException {
    flush();
    IOUtils.closeQuietly(fos);
  }

  @Override
  public TableStat getStats() {
    if (enabledStats) {
      return stats.getTableStat();
    } else {
      return null;
    }
  }
}
