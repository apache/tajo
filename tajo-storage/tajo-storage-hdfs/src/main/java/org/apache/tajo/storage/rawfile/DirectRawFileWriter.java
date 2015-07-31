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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.storage.FileAppender;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.TableStatistics;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.BaseTupleBuilder;
import org.apache.tajo.tuple.offheap.OffHeapRowBlock;
import org.apache.tajo.tuple.offheap.UnSafeTuple;
import org.apache.tajo.unit.StorageUnit;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DirectRawFileWriter extends FileAppender {
  public static final String FILE_EXTENSION = "draw";
  private static final Log LOG = LogFactory.getLog(DirectRawFileWriter.class);

  private FileChannel channel;
  private RandomAccessFile randomAccessFile;
  private FSDataOutputStream fos;
  private TajoDataTypes.DataType[] columnTypes;
  private boolean isLocal;
  private long pos;

  private TableStatistics stats;

  private BaseTupleBuilder builder;

  public DirectRawFileWriter(Configuration conf, TaskAttemptId taskAttemptId,
                             final Schema schema, final TableMeta meta, final Path path) throws IOException {
    super(conf, taskAttemptId, schema, meta, path);
  }

  @Override
  public void init() throws IOException {
    File file;
    FileSystem fs = path.getFileSystem(conf);

    if (fs instanceof LocalFileSystem) {
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
      isLocal = true;
    } else {
      fos = fs.create(path, true);
      isLocal = false;
    }

    pos = 0;
    columnTypes = new TajoDataTypes.DataType[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      columnTypes[i] = schema.getColumn(i).getDataType();
    }

    if (enabledStats) {
      this.stats = new TableStatistics(this.schema);
    }

    builder = new BaseTupleBuilder(schema);

    super.init();
  }

  @Override
  public long getOffset() throws IOException {
    return pos;
  }

  private long getFilePosition() throws IOException {
    if (isLocal) {
      return channel.position();
    } else {
      return fos.getPos();
    }
  }

  public void writeRowBlock(OffHeapRowBlock rowBlock) throws IOException {
    write(rowBlock.nioBuffer());
    if (enabledStats) {
      stats.incrementRows(rowBlock.rows());
    }

    pos = getFilePosition();
  }

  private ByteBuffer buffer;
  private void ensureSize(int size) throws IOException {
    if (buffer.remaining() < size) {

      buffer.limit(buffer.position());
      buffer.flip();
      write(buffer);

      buffer.clear();
    }
  }

  private void write(ByteBuffer buffer) throws IOException {
    if(isLocal) {
      channel.write(buffer);
    } else {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      fos.write(bytes);
    }
  }

  @Override
  public void addTuple(Tuple t) throws IOException {
    if (enabledStats) {
      for (int i = 0; i < schema.size(); i++) {
        stats.analyzeField(i, t);
      }
    }

    if (buffer == null) {
      buffer = ByteBuffer.allocateDirect(64 * StorageUnit.KB);
    }

    UnSafeTuple unSafeTuple;

    if (!(t instanceof UnSafeTuple)) {
      RowStoreUtil.convert(t, builder);
      unSafeTuple = builder.buildToZeroCopyTuple();
    } else {
      unSafeTuple = (UnSafeTuple) t;
    }

    ByteBuffer bb = unSafeTuple.nioBuffer();
    ensureSize(bb.limit());
    buffer.put(bb);

    pos = getFilePosition() + (buffer.limit() - buffer.remaining());

    if (enabledStats) {
      stats.incrementRow();
    }
  }

  @Override
  public void flush() throws IOException {
    if (buffer != null) {
      buffer.limit(buffer.position());
      buffer.flip();
      write(buffer);
      buffer.clear();
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

    IOUtils.cleanup(LOG, channel, randomAccessFile, fos);
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
