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

import com.google.protobuf.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatumFactory;
import org.apache.tajo.storage.FileScanner;
import org.apache.tajo.storage.SeekableScanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.directmem.RowOrientedRowBlock;
import org.apache.tajo.storage.directmem.SizeOf;
import org.apache.tajo.storage.directmem.UnSafeTuple;
import org.apache.tajo.storage.directmem.UnsafeUtil;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.BitArray;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DirectRawFileScanner extends FileScanner implements SeekableScanner {
  private static final Log LOG = LogFactory.getLog(DirectRawFileScanner.class);

  private FileChannel channel;
  private TajoDataTypes.DataType[] columnTypes;
  private Path path;

  private ByteBuffer buffer;
  private UnSafeTuple tuple;

  private int headerSize = 0;
  private static final int RECORD_SIZE = 4;
  private boolean eof = false;
  private long fileSize;
  private FileInputStream fis;
  private long recordCount;

  public DirectRawFileScanner(Configuration conf, Schema schema, TableMeta meta, Path path) throws IOException {
    super(conf, schema, meta, null);
    this.path = path;
  }

  @SuppressWarnings("unused")
  public DirectRawFileScanner(Configuration conf, Schema schema, TableMeta meta, FileFragment fragment) throws IOException {
    this(conf, schema, meta, fragment.getPath());
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

    fis = new FileInputStream(file);
    channel = fis.getChannel();
    fileSize = channel.size();

    if (tableStats != null) {
      tableStats.setNumBytes(fileSize);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("RawFileScanner open:" + path + "," + channel.position() + ", size :" + channel.size());
    }

    buffer = ByteBuffer.allocateDirect(64 * 1024);

    columnTypes = new TajoDataTypes.DataType[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      columnTypes[i] = schema.getColumn(i).getDataType();
    }

    // initial read
    channel.read(buffer);
    buffer.flip();

    super.init();
  }

  @Override
  public long getNextOffset() throws IOException {
    return channel.position() - buffer.remaining();
  }

  @Override
  public void seek(long offset) throws IOException {
    long currentPos = channel.position();
    if(currentPos < offset &&  offset < currentPos + buffer.limit()){
      buffer.position((int)(offset - currentPos));
    } else {
      buffer.clear();
      channel.position(offset);
      channel.read(buffer);
      buffer.flip();
      eof = false;
    }
  }

  private boolean fillBuffer() throws IOException {
    buffer.compact();
    if (channel.read(buffer) == -1) {
      eof = true;
      return false;
    } else {
      buffer.flip();
      return true;
    }
  }

  private boolean ensureReadSize(int size) throws IOException {
    if (buffer.remaining() < size) {
      if (!fillBuffer()) {
        return false;
      }
    }

    return true;
  }

  public boolean next(RowOrientedRowBlock rowblock) throws IOException {
    if (eof) {
      return false;
    }

    int rowIdx = 0;
    while (rowIdx < rowblock.totalRowNum()) {

      ensureReadSize(SizeOf.SIZE_OF_INT); // guarantee the record length header size
      int recordBytesLen = buffer.getInt();
      ensureReadSize(recordBytesLen);
      rowblock.copyRowRecord(buffer, recordBytesLen);

      rowIdx++;
    }

    return true;
  }

  @Override
  public Tuple next() throws IOException {
    if(eof) return null;

    if (buffer.remaining() < headerSize) {
      if (!fillBuffer()) {
        return null;
      }
    }

    // backup the buffer state
    int bufferLimit = buffer.limit();
    int recordSize = buffer.getInt();

    // restore the start of record contents
    buffer.limit(bufferLimit);
    //buffer.position(recordOffset + headerSize);
    if (buffer.remaining() < (recordSize - headerSize)) {
      if (!fillBuffer()) {
        return null;
      }
    }

    recordCount++;

    if(!buffer.hasRemaining() && channel.position() == fileSize){
      eof = true;
    }
    return new VTuple(tuple);
  }

  @Override
  public void reset() throws IOException {
    // clear the buffer
    buffer.clear();
    // reload initial buffer
    channel.position(0);
    channel.read(buffer);
    buffer.flip();
    eof = false;
  }

  @Override
  public void close() throws IOException {
    if (tableStats != null) {
      tableStats.setReadBytes(fileSize);
      tableStats.setNumRows(recordCount);
    }

    UnsafeUtil.free(buffer);
    IOUtils.cleanup(LOG, channel, fis);
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
  public boolean isSplittable(){
    return false;
  }

  @Override
  public float getProgress() {
    try {
      tableStats.setNumRows(recordCount);
      long filePos = 0;
      if (channel != null) {
        filePos = channel.position();
        tableStats.setReadBytes(filePos);
      }

      if(eof || channel == null) {
        tableStats.setReadBytes(fileSize);
        return 1.0f;
      }

      if (filePos == 0) {
        return 0.0f;
      } else {
        return Math.min(1.0f, ((float)filePos / (float)fileSize));
      }
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      return 0.0f;
    }
  }
}
