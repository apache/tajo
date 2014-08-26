/*
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

package org.apache.tajo.storage.rawfile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.storage.FileAppender;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.TableStatistics;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.directmem.RowOrientedRowBlock;
import org.apache.tajo.storage.directmem.UnSafeTuple;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class DirectRawFileWriter extends FileAppender {
  private static final Log LOG = LogFactory.getLog(DirectRawFileWriter.class);

  private FileChannel channel;
  private RandomAccessFile randomAccessFile;
  private TajoDataTypes.DataType[] columnTypes;
  private long pos;

  private TableStatistics stats;

  public DirectRawFileWriter(Configuration conf, Schema schema, TableMeta meta, Path path) throws IOException {
    super(conf, schema, meta, path);
  }

  public void init() throws IOException {
    File file;
    try {
      if (path.toUri().getScheme() != null) {
        file = new File(path.toUri());
      } else {
        file = new File(path.toString());
      }
    } catch (IllegalArgumentException iae) {
      throw new IOException(iae);
    }

    randomAccessFile = new RandomAccessFile(file, "rw");
    channel = randomAccessFile.getChannel();
    pos = 0;

    columnTypes = new TajoDataTypes.DataType[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      columnTypes[i] = schema.getColumn(i).getDataType();
    }

    if (enabledStats) {
      this.stats = new TableStatistics(this.schema);
    }

    super.init();
  }

  @Override
  public long getOffset() throws IOException {
    return pos;
  }

  public void writeRowBlock(RowOrientedRowBlock rowBlock) throws IOException {
    channel.write(rowBlock.nioBuffer());
    if (enabledStats) {
      stats.incrementRows(rowBlock.rows());
    }

    pos = channel.position();
  }

  private RowStoreUtil.DirectRowStoreEncoder encoder;

  @Override
  public void addTuple(Tuple t) throws IOException {
    if (enabledStats) {
      for (int i = 0; i < schema.size(); i++) {
        stats.analyzeField(i, t.get(i));
      }
    }

    if (t instanceof UnSafeTuple) {
      UnSafeTuple unSafeTuple = (UnSafeTuple) t;

      channel.write(unSafeTuple.nioBuffer());

      pos = channel.position();
    } else {

      if (encoder == null) {
        encoder = RowStoreUtil.createDirectRawEncoder(schema);
      }

      channel.write(encoder.encode(t));

      pos = channel.position();
    }

    if (enabledStats) {
      stats.incrementRow();
    }
  }

  @Override
  public void flush() throws IOException {
  }

  @Override
  public void close() throws IOException {
    flush();
    if (enabledStats) {
      stats.setNumBytes(getOffset());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("RawFileAppender written: " + getOffset() + " bytes, path: " + path);
    }

    IOUtils.cleanup(LOG, channel, randomAccessFile);
  }

  @Override
  public TableStats getStats() {
    if (enabledStats) {
      stats.setNumBytes(pos);
      return stats.getTableStat();
    } else {
      return null;
    }
  }
}
