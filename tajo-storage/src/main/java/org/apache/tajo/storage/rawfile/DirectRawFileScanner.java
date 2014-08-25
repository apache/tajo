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
  private Tuple tuple;

  private int headerSize = 0;
  private BitArray nullFlags;
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

    tuple = new VTuple(columnTypes.length);

    // initial read
    channel.read(buffer);
    buffer.flip();

    nullFlags = new BitArray(schema.size());
    headerSize = RECORD_SIZE + 2 + nullFlags.bytesLength();

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

  /**
   * Decode a ZigZag-encoded 32-bit value.  ZigZag encodes signed integers
   * into values that can be efficiently encoded with varint.  (Otherwise,
   * negative values must be sign-extended to 64 bits to be varint encoded,
   * thus always taking 10 bytes on the wire.)
   *
   * @param n An unsigned 32-bit integer, stored in a signed int because
   *          Java has no explicit unsigned support.
   * @return A signed 32-bit integer.
   */
  public static int decodeZigZag32(final int n) {
    return (n >>> 1) ^ -(n & 1);
  }

  /**
   * Decode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
   * into values that can be efficiently encoded with varint.  (Otherwise,
   * negative values must be sign-extended to 64 bits to be varint encoded,
   * thus always taking 10 bytes on the wire.)
   *
   * @param n An unsigned 64-bit integer, stored in a signed int because
   *          Java has no explicit unsigned support.
   * @return A signed 64-bit integer.
   */
  public static long decodeZigZag64(final long n) {
    return (n >>> 1) ^ -(n & 1);
  }


  /**
   * Read a raw Varint from the stream.  If larger than 32 bits, discard the
   * upper bits.
   */
  public int readRawVarint32() throws IOException {
    byte tmp = buffer.get();
    if (tmp >= 0) {
      return tmp;
    }
    int result = tmp & 0x7f;
    if ((tmp = buffer.get()) >= 0) {
      result |= tmp << 7;
    } else {
      result |= (tmp & 0x7f) << 7;
      if ((tmp = buffer.get()) >= 0) {
        result |= tmp << 14;
      } else {
        result |= (tmp & 0x7f) << 14;
        if ((tmp = buffer.get()) >= 0) {
          result |= tmp << 21;
        } else {
          result |= (tmp & 0x7f) << 21;
          result |= (tmp = buffer.get()) << 28;
          if (tmp < 0) {
            // Discard upper 32 bits.
            for (int i = 0; i < 5; i++) {
              if (buffer.get() >= 0) {
                return result;
              }
            }
            throw new IOException("Invalid Variable int32");
          }
        }
      }
    }
    return result;
  }

  /** Read a raw Varint from the stream. */
  public long readRawVarint64() throws IOException {
    int shift = 0;
    long result = 0;
    while (shift < 64) {
      final byte b = buffer.get();
      result |= (long)(b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
      shift += 7;
    }
    throw new IOException("Invalid Variable int64");
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
    int nullFlagSize = buffer.getShort();

    buffer.limit(buffer.position() + nullFlagSize);
    nullFlags.fromByteBuffer(buffer);
    // restore the start of record contents
    buffer.limit(bufferLimit);
    //buffer.position(recordOffset + headerSize);
    if (buffer.remaining() < (recordSize - headerSize)) {
      if (!fillBuffer()) {
        return null;
      }
    }

    recordCount++;

    for (int i = 0; i < columnTypes.length; i++) {
      // check if the i'th column is null
      if (nullFlags.get(i)) {
        tuple.put(i, DatumFactory.createNullDatum());
        continue;
      }

      switch (columnTypes[i].getType()) {
      case BOOLEAN :
        tuple.put(i, DatumFactory.createBool(buffer.get()));
        break;

      case BIT :
        tuple.put(i, DatumFactory.createBit(buffer.get()));
        break;

      case CHAR :
        int realLen = readRawVarint32();
        byte[] buf = new byte[realLen];
        buffer.get(buf);
        tuple.put(i, DatumFactory.createChar(buf));
        break;

      case INT2 :
        tuple.put(i, DatumFactory.createInt2(buffer.getShort()));
        break;

      case INT4 :
        tuple.put(i, DatumFactory.createInt4(decodeZigZag32(readRawVarint32())));
        break;

      case INT8 :
        tuple.put(i, DatumFactory.createInt8(decodeZigZag64(readRawVarint64())));
        break;

      case FLOAT4 :
        tuple.put(i, DatumFactory.createFloat4(buffer.getFloat()));
        break;

      case FLOAT8 :
        tuple.put(i, DatumFactory.createFloat8(buffer.getDouble()));
        break;

      case TEXT : {
        int len = readRawVarint32();
        byte [] strBytes = new byte[len];
        buffer.get(strBytes);
        tuple.put(i, DatumFactory.createText(new String(strBytes)));
        break;
      }

      case BLOB : {
        int len = readRawVarint32();
        byte [] rawBytes = new byte[len];
        buffer.get(rawBytes);
        tuple.put(i, DatumFactory.createBlob(rawBytes));
        break;
      }

      case PROTOBUF: {
        int len = readRawVarint32();
        byte [] rawBytes = new byte[len];
        buffer.get(rawBytes);

        ProtobufDatumFactory factory = ProtobufDatumFactory.get(columnTypes[i]);
        Message.Builder builder = factory.newBuilder();
        builder.mergeFrom(rawBytes);
        tuple.put(i, factory.createDatum(builder.build()));
        break;
      }

      case INET4 :
        byte [] ipv4Bytes = new byte[4];
        buffer.get(ipv4Bytes);
        tuple.put(i, DatumFactory.createInet4(ipv4Bytes));
        break;

      case DATE: {
        int val = buffer.getInt();
        if (val < Integer.MIN_VALUE + 1) {
          tuple.put(i, DatumFactory.createNullDatum());
        } else {
          tuple.put(i, DatumFactory.createFromInt4(columnTypes[i], val));
        }
        break;
      }
      case TIME:
      case TIMESTAMP: {
        long val = buffer.getLong();
        if (val < Long.MIN_VALUE + 1) {
          tuple.put(i, DatumFactory.createNullDatum());
        } else {
          tuple.put(i, DatumFactory.createFromInt8(columnTypes[i], val));
        }
        break;
      }
      case NULL_TYPE:
        tuple.put(i, NullDatum.get());
        break;

      default:
      }
    }

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
