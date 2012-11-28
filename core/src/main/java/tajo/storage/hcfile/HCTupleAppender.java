/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.storage.hcfile;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.CompressType;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.storage.Tuple;
import tajo.storage.exception.UnknownCodecException;
import tajo.storage.exception.UnknownDataTypeException;
import tajo.storage.hcfile.HCFile.Appender;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

public class HCTupleAppender implements TupleAppender {
  public static final String META_FILE_NAME = ".index";
  private final Log LOG = LogFactory.getLog(HCTupleAppender.class);
  private Configuration conf;
  private Schema schema;
  private final Path tablePath;
  private long shardId;
  private Map<Column, Appender> columnAppenderMap = Maps.newHashMap();
  private Map<Column, Integer> columnFileIdMap = Maps.newHashMap();
//  private Set<String> unfinishedFiles = Sets.newHashSet();
  private Column baseColumn;
  private Map<Column, Index<String>> columnIndexMap = Maps.newHashMap();

  public HCTupleAppender(Configuration conf, TableMeta meta, Column baseColumn, Path tablePath)
      throws IOException, UnknownCodecException, UnknownDataTypeException {
    this.conf = conf;
    this.schema = meta.getSchema();
    this.baseColumn = baseColumn;
    this.tablePath = tablePath;
    this.shardId = -1;
    init();
  }

  private void init() throws IOException, UnknownCodecException,
      UnknownDataTypeException {
    Column column;
    newShardId();

    for (int i = 0; i < schema.getColumnNum(); i++) {
      column = schema.getColumn(i);
      columnIndexMap.put(column, new Index<String>());
      columnAppenderMap.put(column, newAppender(column));
    }
  }

  @Override
  public void addTuple(Tuple t) throws IOException {
    Appender appender;
    for (int i = 0; i < schema.getColumnNum(); i++) {
      appender = columnAppenderMap.get(schema.getColumn(i));
      if (!appender.isAppendable(t.get(i))) {
        try {
          Column col = schema.getColumn(i);
          appender = newAppender(col);
          columnAppenderMap.put(col, appender);
        } catch (UnknownDataTypeException e) {
          LOG.info(e);
        } catch (UnknownCodecException e) {
          LOG.info(e);
        }
      }
      appender.append(t.get(i));
    }
  }

  private Appender newAppender(Column column)
      throws UnknownCodecException, IOException, UnknownDataTypeException {
    long nextStartId;

    if (columnAppenderMap.containsKey(column)) {
//      unfinishedFiles.remove(colName);
//      if (unfinishedFiles.isEmpty()) {
      if (column.equals(baseColumn)) {
        newShardId();
      }
      Appender oldAppender = columnAppenderMap.get(column);
      nextStartId = oldAppender.getStartId() + oldAppender.getRecordNum();
      oldAppender.close();
    } else {
      nextStartId = 0;
    }

    ColumnMeta columnMeta = newColumnMeta(nextStartId, column.getDataType());
    Path columnPath = new Path(tablePath, getColumnFileName(column));
    LOG.info("new appender is initialized for " + column.getColumnName());
    LOG.info("column path:  " + columnPath.toString());
    Appender newAppender = new Appender(conf, columnMeta, columnPath);
    Index<String> index = columnIndexMap.get(column);
    index.add(new IndexItem(newAppender.getStartId(),
        columnPath.getName()));
    return newAppender;
  }

  private ColumnMeta newColumnMeta(long startId, DataType dataType) {
    return new HColumnMetaWritable(startId, dataType, CompressType.COMP_NONE,
        false, false, true);
  }

  private String getColumnFileName(Column column) {
    int fileId;
    if (columnFileIdMap.containsKey(column)) {
      fileId = columnFileIdMap.get(column) + 1;
    } else {
      fileId = 0;
    }
    columnFileIdMap.put(column, fileId);
    return column.getColumnName() + "/" + shardId + "_" + fileId;
  }

  private void newShardId() {
    ++shardId;
    columnFileIdMap.clear();
//    for (Column col : schema.getColumns()) {
//      unfinishedFiles.add(col.getColumnName());
//    }
    LOG.info("new shard id: " + shardId);
  }

  @Override
  public void flush() throws IOException {
    for (Appender appender : columnAppenderMap.values()) {
      appender.flush();
    }
  }

  @Override
  public void close() throws IOException {
    for (Appender appender : columnAppenderMap.values()) {
      appender.close();
    }
    writeIndex();
  }

  private Path getColumnMetaPath(String colName) {
    Path columnPath = new Path(tablePath, colName);
    return new Path(columnPath, META_FILE_NAME);
  }

  private void writeIndex() throws IOException {
    FileSystem fs = tablePath.getFileSystem(conf);
    for (Entry<Column, Index<String>> e : columnIndexMap.entrySet()) {
      Path metaPath = getColumnMetaPath(e.getKey().getColumnName());
      Index<String> index = e.getValue();
      FSDataOutputStream out = fs.create(metaPath);
      for (IndexItem<String> item : index.get()) {
        out.writeLong(item.getRid());
        byte[] bytes = item.getValue().getBytes();
        out.writeShort(bytes.length);
        out.write(bytes);
      }
      out.close();
    }
  }
}
