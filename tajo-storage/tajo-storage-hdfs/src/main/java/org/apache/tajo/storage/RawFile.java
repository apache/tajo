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

import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatumFactory;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.rawfile.DirectRawFileWriter;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.BitArray;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public class RawFile {
  private static final Log LOG = LogFactory.getLog(RawFile.class);
  public static final String READ_BUFFER_SIZE = "tajo.storage.raw.io.read-buffer.bytes";
  public static final String WRITE_BUFFER_SIZE = "tajo.storage.raw.io.write-buffer.bytes";
  public static final int DEFAULT_BUFFER_SIZE = 128 * StorageUnit.KB;

  public static class RawFileScanner extends FileScanner implements SeekableScanner {
    private FileChannel channel;
    private DataType[] columnTypes;

    private ByteBuffer buffer;
    private ByteBuf buf;
    private Tuple outTuple;

    private int headerSize = 0; // Header size of a tuple
    private BitArray nullFlags;
    private static final int RECORD_SIZE = 4;
    private boolean eos = false;
    private long startOffset;
    private long endOffset;
    private FileInputStream fis;
    private long recordCount;
    private long totalReadBytes;
    private long filePosition;
    private boolean forceFillBuffer;

    public RawFileScanner(Configuration conf, Schema schema, TableMeta meta, Fragment fragment) throws IOException {
      super(conf, schema, meta, fragment);
    }

    @Override
    public void init() throws IOException {
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
      fis = new FileInputStream(file);
      channel = fis.getChannel();
      filePosition = startOffset = fragment.getStartKey();
      endOffset = fragment.getStartKey() + fragment.getLength();

      if (LOG.isDebugEnabled()) {
        LOG.debug("RawFileScanner open:" + fragment + "," + channel.position() + ", file size :" + channel.size()
            + ", fragment length :" + fragment.getLength());
      }

      if(buf == null) {
        buf = BufferPool.directBuffer(conf.getInt(READ_BUFFER_SIZE, DEFAULT_BUFFER_SIZE)).order(ByteOrder.LITTLE_ENDIAN);
        buffer = buf.nioBuffer(0, buf.capacity());
      }

      columnTypes = new DataType[schema.size()];
      for (int i = 0; i < schema.size(); i++) {
        columnTypes[i] = schema.getColumn(i).getDataType();
      }

      outTuple = new VTuple(columnTypes.length);
      nullFlags = new BitArray(schema.size());
      headerSize = RECORD_SIZE + 2 + nullFlags.bytesLength(); // The middle 2 bytes is for NullFlagSize

      // initial set position
      if (fragment.getStartKey() > 0) {
        channel.position(fragment.getStartKey());
      }

      forceFillBuffer = true;
      super.init();
    }

    @Override
    public long getNextOffset() throws IOException {
      return filePosition - (forceFillBuffer ? 0 : buffer.remaining());
    }

    @Override
    public void seek(long offset) throws IOException {
      eos = false;
      filePosition = channel.position();

      // do not fill the buffer if the offset is already included in the buffer.
      if(!forceFillBuffer && filePosition > offset && offset > filePosition - buffer.limit()){
        buffer.position((int)(offset - (filePosition - buffer.limit())));
      } else {
        if(offset < startOffset || offset > startOffset + fragment.getLength()){
          throw new IndexOutOfBoundsException(String.format("range(%d, %d), offset: %d",
              startOffset, startOffset + fragment.getLength(), offset));
        }
        channel.position(offset);
        filePosition = offset;
        buffer.clear();
        forceFillBuffer = true;
        fillBuffer();
      }
    }

    private boolean fillBuffer() throws IOException {
      if(!forceFillBuffer) buffer.compact();

      int bytesRead = channel.read(buffer);
      forceFillBuffer = false;
      if (bytesRead == -1) {
        eos = true;
        return false;
      } else {
        buffer.flip(); //The limit is set to the current filePosition and then the filePosition is set to zero
        filePosition += bytesRead;
        totalReadBytes += bytesRead;
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
      if(eos) return null;

      if (forceFillBuffer || buffer.remaining() < headerSize) {
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
      if (buffer.remaining() < (recordSize - headerSize)) {

        //if the buffer reaches the writable size, the buffer increase the record size
        reSizeBuffer(recordSize);

        if (!fillBuffer()) {
          return null;
        }
      }

      for (int i = 0; i < columnTypes.length; i++) {
        // check if the i'th column is null
        if (nullFlags.get(i)) {
          outTuple.put(i, DatumFactory.createNullDatum());
          continue;
        }

        switch (columnTypes[i].getType()) {
        case BOOLEAN:
          outTuple.put(i, DatumFactory.createBool(buffer.get()));
          break;

        case BIT:
          outTuple.put(i, DatumFactory.createBit(buffer.get()));
          break;

        case CHAR:
          int realLen = readRawVarint32();
          byte[] buf = new byte[realLen];
          buffer.get(buf);
          outTuple.put(i, DatumFactory.createChar(buf));
          break;

        case INT2:
          outTuple.put(i, DatumFactory.createInt2(buffer.getShort()));
          break;

        case INT4:
          outTuple.put(i, DatumFactory.createInt4(decodeZigZag32(readRawVarint32())));
          break;

        case INT8:
          outTuple.put(i, DatumFactory.createInt8(decodeZigZag64(readRawVarint64())));
          break;

        case FLOAT4:
          outTuple.put(i, DatumFactory.createFloat4(buffer.getFloat()));
          break;

        case FLOAT8:
          outTuple.put(i, DatumFactory.createFloat8(buffer.getDouble()));
          break;

        case TEXT: {
          int len = readRawVarint32();
          byte[] strBytes = new byte[len];
          buffer.get(strBytes);
          outTuple.put(i, DatumFactory.createText(strBytes));
          break;
        }

        case BLOB: {
          int len = readRawVarint32();
          byte[] rawBytes = new byte[len];
          buffer.get(rawBytes);
          outTuple.put(i, DatumFactory.createBlob(rawBytes));
          break;
        }

        case PROTOBUF: {
          int len = readRawVarint32();
          byte[] rawBytes = new byte[len];
          buffer.get(rawBytes);

          outTuple.put(i, ProtobufDatumFactory.createDatum(columnTypes[i], rawBytes));
          break;
        }

        case INET4:
          outTuple.put(i, DatumFactory.createInet4(buffer.getInt()));
          break;

        case DATE: {
          int val = buffer.getInt();
          if (val < Integer.MIN_VALUE + 1) {
            outTuple.put(i, DatumFactory.createNullDatum());
          } else {
            outTuple.put(i, DatumFactory.createFromInt4(columnTypes[i], val));
          }
          break;
        }
        case TIME:
        case TIMESTAMP: {
          long val = buffer.getLong();
          if (val < Long.MIN_VALUE + 1) {
            outTuple.put(i, DatumFactory.createNullDatum());
          } else {
            outTuple.put(i, DatumFactory.createFromInt8(columnTypes[i], val));
          }
          break;
        }
        case NULL_TYPE:
          outTuple.put(i, NullDatum.get());
          break;

        default:
        }
      }

      recordCount++;

      if(filePosition - buffer.remaining() >= endOffset){
        eos = true;
      }
      return outTuple;
    }

    private void reSizeBuffer(int writableBytes){
      if (buffer.capacity() - buffer.remaining()  <  writableBytes) {
        buf.setIndex(buffer.position(), buffer.limit());
        buf.markReaderIndex();
        buf.discardReadBytes();
        buf.ensureWritable(writableBytes);
        buffer = buf.nioBuffer(0, buf.capacity());
        buffer.limit(buf.writerIndex());
      }
    }

    @Override
    public void reset() throws IOException {
      // reset the buffer
      buffer.clear();
      forceFillBuffer = true;
      filePosition = fragment.getStartKey();
      recordCount = 0;
      channel.position(filePosition);
      eos = false;
    }

    @Override
    public void close() throws IOException {
      if(buf != null){
        buffer.clear();
        buffer = null;

        buf.release();
        buf = null;
      }

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
    public void setFilter(EvalNode filter) {
      throw new TajoRuntimeException(new UnsupportedException());
    }

    @Override
    public boolean isSplittable(){
      return false;
    }

    @Override
    public TableStats getInputStats() {
      if(tableStats != null){
        tableStats.setNumRows(recordCount);
        tableStats.setReadBytes(totalReadBytes); // actual read bytes (scan + rescan * n)
        tableStats.setNumBytes(fragment.getLength());
      }
      return tableStats;
    }

    @Override
    public float getProgress() {
      if(eos) {
        return 1.0f;
      }

      long readBytes = filePosition - startOffset;
      if (readBytes == 0) {
        return 0.0f;
      } else {
        return Math.min(1.0f, ((float) readBytes / fragment.getLength()));
      }
    }
  }

  public static class RawFileAppender extends DirectRawFileWriter {

    public RawFileAppender(Configuration conf, TaskAttemptId taskAttemptId, Schema schema,
                           TableMeta meta, Path workDir) throws IOException {
      super(conf, taskAttemptId, schema, meta, workDir, null);
    }
  }
}
