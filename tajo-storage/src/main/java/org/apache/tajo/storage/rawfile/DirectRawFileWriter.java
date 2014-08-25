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
import org.apache.tajo.storage.TableStatistics;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.directmem.RowOrientedRowBlock;
import org.apache.tajo.storage.directmem.UnSafeTuple;
import org.apache.tajo.storage.directmem.UnsafeUtil;
import org.apache.tajo.util.BitArray;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DirectRawFileWriter extends FileAppender {
  private static final Log LOG = LogFactory.getLog(DirectRawFileWriter.class);

  private FileChannel channel;
  private RandomAccessFile randomAccessFile;
  private TajoDataTypes.DataType[] columnTypes;

  private ByteBuffer buffer;
  private BitArray nullFlags;
  private int headerSize = 0;
  private static final int RECORD_SIZE = 4;
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

    buffer = ByteBuffer.allocateDirect(64 * 1024);

    // comput the number of bytes, representing the null flags

    nullFlags = new BitArray(schema.size());
    headerSize = RECORD_SIZE + 2 + nullFlags.bytesLength();

    if (enabledStats) {
      this.stats = new TableStatistics(this.schema);
    }

    super.init();
  }

  @Override
  public long getOffset() throws IOException {
    return pos;
  }

  private void flushBuffer() throws IOException {
    buffer.limit(buffer.position());
    buffer.flip();
    channel.write(buffer);
    buffer.clear();
  }

  private boolean flushBufferAndReplace(int recordOffset, int sizeToBeWritten)
      throws IOException {

    // if the buffer reaches the limit,
    // write the bytes from 0 to the previous record.
    if (buffer.remaining() < sizeToBeWritten) {

      int limit = buffer.position();
      buffer.limit(recordOffset);
      buffer.flip();
      channel.write(buffer);
      buffer.position(recordOffset);
      buffer.limit(limit);
      buffer.compact();

      return true;
    } else {
      return false;
    }
  }

  public void writeRowBlock(RowOrientedRowBlock rowBlock) throws IOException {
    channel.write(rowBlock.byteBuffer());
    stats.incrementRows(rowBlock.totalRowNum());
  }

  @Override
  public void addTuple(Tuple t) throws IOException {
  }

  @Override
  public void flush() throws IOException {
    if(buffer != null){
      flushBuffer();
    }
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

    UnsafeUtil.free(buffer);
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
