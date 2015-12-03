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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.tuple.RowBlockReader;
import org.apache.tajo.tuple.memory.MemoryRowBlock;
import org.apache.tajo.tuple.memory.RowBlock;
import org.apache.tajo.tuple.memory.UnSafeTuple;
import org.apache.tajo.unit.StorageUnit;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class DirectRawFileScanner extends FileScanner implements SeekableScanner {
  private static final Log LOG = LogFactory.getLog(DirectRawFileScanner.class);

  public static final String READ_BUFFER_SIZE = "tajo.storage.raw.io.read-buffer.bytes";
  public static final int DEFAULT_BUFFER_SIZE = 128 * StorageUnit.KB;

  private SeekableInputChannel channel;

  private boolean eos = false;
  private long recordCount;
  private long filePosition;
  private long endOffset;

  private UnSafeTuple unSafeTuple = new UnSafeTuple();
  private RowBlock tupleBuffer;
  private RowBlockReader reader;

  public DirectRawFileScanner(Configuration conf, Schema schema, TableMeta meta, Fragment fragment) throws IOException {
    super(conf, schema, meta, fragment);
  }

  @Override
  public void init() throws IOException {
    initChannel();

    if (tupleBuffer == null) {
      tupleBuffer = new MemoryRowBlock(SchemaUtil.toDataTypes(schema),
          conf.getInt(READ_BUFFER_SIZE, DEFAULT_BUFFER_SIZE));
    } else {
      tupleBuffer.clear();
    }

    fetchNeeded = true;
    eos = false;
    super.init();
  }

  private void initChannel() throws IOException {
    FileSystem fs = FileScanner.getFileSystem((TajoConf) conf, fragment.getPath());

    if (fs instanceof LocalFileSystem) {
      File file;
      try {
        if (fragment.getPath().toUri().getScheme() != null) {
          file = new File(fragment.getPath().toUri());
        } else {
          file = new File(fragment.getPath().toString());
        }
      } catch (IllegalArgumentException iae) {
        throw new IOException(iae);
      }

      channel = new LocalFileInputChannel(new FileInputStream(file));
    } else {
      channel = new FSDataInputChannel(fs.open(fragment.getPath()));
    }

    // initial set position
    if (fragment.getStartKey() > 0) {
      channel.seek(fragment.getStartKey());
    }

    filePosition = fragment.getStartKey();
    endOffset = fragment.getStartKey() + fragment.getLength();
    if (LOG.isDebugEnabled()) {
      LOG.debug("RawFileScanner open:" + fragment.getPath() + ", offset :" +
          fragment.getStartKey() + ", fragment length :" + fragment.getLength());
    }
  }

  @Override
  public long getNextOffset() throws IOException {
    return filePosition - reader.remainForRead();
  }

  @Override
  public void seek(long offset) throws IOException {
    channel.seek(offset);
    filePosition = channel.position();
    tupleBuffer.clear();
    fetchNeeded = true;
  }

  public boolean next(RowBlock rowblock) throws IOException {
    long reamin = reader == null ? 0 : reader.remainForRead();
    boolean ret = rowblock.copyFromChannel(channel);
    reader = rowblock.getReader();
    filePosition += rowblock.getMemory().writerPosition() - reamin;
    return ret;
  }

  private boolean fetchNeeded = true;

  @Override
  public UnSafeTuple next() throws IOException {
    if(eos) {
      return null;
    }

    while(true) {
      if (fetchNeeded) {
        if (!next(tupleBuffer)) {
          return null;
        }
      }

      fetchNeeded = !reader.next(unSafeTuple);

      if (!fetchNeeded) {
        recordCount++;
        if(filePosition - reader.remainForRead() >= endOffset){
          eos = true;
        }
        return unSafeTuple;
      }
    }
  }

  @Override
  public void reset() throws IOException {
    // reload initial buffer
    filePosition = fragment.getStartKey();
    recordCount = 0;
    seek(filePosition);
    eos = false;
  }

  @Override
  public void close() throws IOException {
    if (tableStats != null) {
      tableStats.setReadBytes(filePosition - fragment.getStartKey());
      tableStats.setNumRows(recordCount);
    }
    if(tupleBuffer != null) {
      tupleBuffer.release();
      tupleBuffer = null;
    }
    reader = null;

    IOUtils.cleanup(LOG, channel);
  }

  @Override
  public boolean isProjectable() {
    return false;
  }

  @Override
  public boolean isSelectable() {
    return false;
  }

  @Override
  public void setFilter(EvalNode filter) {

  }

  @Override
  public boolean isSplittable(){
    return false;
  }

  @Override
  public TableStats getInputStats() {
    if(tableStats != null){
      tableStats.setNumRows(recordCount);
      tableStats.setReadBytes(filePosition - fragment.getStartKey()); // actual read bytes (scan + rescan * n)
      tableStats.setNumBytes(fragment.getLength());
    }
    return tableStats;
  }

  @Override
  public float getProgress() {
    if(!inited) return 0.0f;

    if(eos) {
      return 1.0f;
    }

    long readBytes = filePosition - fragment.getStartKey();
    if (readBytes == 0) {
      return 0.0f;
    } else {
      return Math.min(1.0f, ((float) readBytes / fragment.getLength()));
    }
  }
}
