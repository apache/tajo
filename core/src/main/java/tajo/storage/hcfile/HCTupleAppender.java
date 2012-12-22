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

public class HCTupleAppender implements TupleAppender {
  private final Log LOG = LogFactory.getLog(HCTupleAppender.class);
  private Configuration conf;
  private Schema schema;
  private final Path dataDir;
  private int shardId;
  private Map<Column, Appender> columnAppenderMap = Maps.newHashMap();
  private Map<Column, Integer> columnFileIdMap = Maps.newHashMap();
  private Column baseColumn;
  private int baseColumnIdx;

  public HCTupleAppender(Configuration conf, TableMeta meta, int baseColumnIdx, Path tablePath)
      throws IOException, UnknownCodecException, UnknownDataTypeException {
    this.conf = conf;
    this.schema = meta.getSchema();
    this.baseColumnIdx = baseColumnIdx;
    this.baseColumn = schema.getColumn(baseColumnIdx);
    this.dataDir = new Path(tablePath, "data");
    this.shardId = -1;
    newShard();
  }

  @Override
  public void addTuple(Tuple t) throws IOException {
    Appender appender = columnAppenderMap.get(baseColumn);

    // try base column
    if (!appender.isAppendable(t.get(baseColumnIdx))) {
      try {
        newShard();
        appender = columnAppenderMap.get(baseColumn);
      } catch (UnknownDataTypeException e) {
        throw new IOException(e);
      } catch (UnknownCodecException e) {
        throw new IOException(e);
      }
    }
    appender.append(t.get(baseColumnIdx));

    for (int i = 0; i < schema.getColumnNum(); i++) {
      if (i == baseColumnIdx) continue;

      appender = columnAppenderMap.get(schema.getColumn(i));
      if (!appender.isAppendable(t.get(i))) {
        try {
          appender = newAppender(schema.getColumn(i));
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
      Appender oldAppender = columnAppenderMap.get(column);
      nextStartId = oldAppender.getStartId() + oldAppender.getRecordNum();
      oldAppender.close();
    } else {
      nextStartId = 0;
    }

    ColumnMeta columnMeta = newColumnMeta(nextStartId, column.getDataType());
    Path columnPath = new Path(dataDir, getColumnFileName(column));
    LOG.info("new appender is initialized for " + column.getColumnName());
    LOG.info("column path:  " + columnPath.toString());
    Appender newAppender = new Appender(conf, columnMeta, columnPath);
    columnAppenderMap.put(column, newAppender);
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
    return shardId + "/" + column.getColumnName() + "_" + fileId;
  }

  private void newShard()
      throws UnknownDataTypeException, IOException, UnknownCodecException {
    ++shardId;
    columnFileIdMap.clear();
    LOG.info("new shard id: " + shardId);
    long oldStartRid = -1, newStartRid = -1;

    if (!columnAppenderMap.isEmpty()) {
      oldStartRid = columnAppenderMap.get(schema.getColumn(0)).getStartId();
    }

    for (Column column : schema.getColumns()) {
      newAppender(column);
    }

    if (oldStartRid != -1) {
      newStartRid = columnAppenderMap.get(schema.getColumn(0)).getStartId();
      writeMeta(shardId-1, oldStartRid, newStartRid-oldStartRid);
    }

//    index.add(new IndexItem(columnAppenderMap.get(schema.getColumn(0)).getStartId(), shardId));
  }

  @Override
  public void flush() throws IOException {
    for (Appender appender : columnAppenderMap.values()) {
      appender.flush();
    }
  }

  @Override
  public void close() throws IOException {
    Appender app = columnAppenderMap.get(schema.getColumn(0));
    writeMeta(shardId, app.getStartId(), app.getRecordNum());
    for (Appender appender : columnAppenderMap.values()) {
      appender.close();
    }
//    writeIndex();
  }

  private void writeMeta(int shardId, long startRid, long length) throws IOException {
    FileSystem fs = dataDir.getFileSystem(conf);
    Path shardPath = new Path(dataDir, shardId+"");
    Path metaPath = new Path(shardPath, ".smeta");
    FSDataOutputStream out = fs.create(metaPath);
    out.writeLong(startRid);
    out.writeLong(length);
    out.close();
  }

/*  private void writeIndex() throws IOException {
    FileSystem fs = dataDir.getFileSystem(conf);
    Path indexPath = new Path(dataDir, ".index");
    FSDataOutputStream out = fs.create(indexPath);
    for (IndexItem<Integer> item : index.get()) {
      out.writeLong(item.getRid());
      out.writeInt(item.getValue());
    }
    out.close();
  }*/
}
