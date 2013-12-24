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

import com.google.protobuf.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatumFactory;
import org.apache.tajo.datum.TimestampDatum;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.BitArray;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class RawFile {
  private static final Log LOG = LogFactory.getLog(RawFile.class);

  public static class RawFileScanner extends FileScanner implements SeekableScanner {
    private FileChannel channel;
    private DataType[] columnTypes;
    private Path path;

    private ByteBuffer buffer;
    private Tuple tuple;

    private int headerSize = 0;
    private BitArray nullFlags;
    private static final int RECORD_SIZE = 4;
    private boolean eof = false;
    private long fileSize;
    private FileInputStream fis;

    public RawFileScanner(Configuration conf, Schema schema, TableMeta meta, Path path) throws IOException {
      super(conf, schema, meta, null);
      this.path = path;
      init();
    }

    @SuppressWarnings("unused")
    public RawFileScanner(Configuration conf, Schema schema, TableMeta meta, FileFragment fragment) throws IOException {
      this(conf, schema, meta, fragment.getPath());
    }

    public void init() throws IOException {
      //Preconditions.checkArgument(FileUtil.isLocalPath(path));
      // TODO - to make it unified one.
      URI uri = path.toUri();
      fis = new FileInputStream(new File(uri));
      channel = fis.getChannel();
      fileSize = channel.size();

      if (LOG.isDebugEnabled()) {
        LOG.debug("RawFileScanner open:" + path + "," + channel.position() + ", size :" + channel.size());
      }

      buffer = ByteBuffer.allocateDirect(128 * 1024);

      columnTypes = new DataType[schema.getColumnNum()];
      for (int i = 0; i < schema.getColumnNum(); i++) {
        columnTypes[i] = schema.getColumn(i).getDataType();
      }

      tuple = new VTuple(columnTypes.length);

      // initial read
      channel.read(buffer);
      buffer.flip();

      nullFlags = new BitArray(schema.getColumnNum());
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
            int realLen = buffer.getInt();
            byte[] buf = new byte[columnTypes[i].getLength()];
            buffer.get(buf);
            byte[] charBuf = Arrays.copyOf(buf, realLen);
            tuple.put(i, DatumFactory.createChar(charBuf));
            break;

          case INT2 :
            tuple.put(i, DatumFactory.createInt2(buffer.getShort()));
            break;

          case INT4 :
            tuple.put(i, DatumFactory.createInt4(buffer.getInt()));
            break;

          case INT8 :
            tuple.put(i, DatumFactory.createInt8(buffer.getLong()));
            break;

          case FLOAT4 :
            tuple.put(i, DatumFactory.createFloat4(buffer.getFloat()));
            break;

          case FLOAT8 :
            tuple.put(i, DatumFactory.createFloat8(buffer.getDouble()));
            break;

          case TEXT :
            // TODO - shoud use CharsetEncoder / CharsetDecoder
            //byte [] rawBytes = getColumnBytes();
            int strSize2 = buffer.getInt();
            byte [] strBytes2 = new byte[strSize2];
            buffer.get(strBytes2);
            tuple.put(i, DatumFactory.createText(new String(strBytes2)));
            break;

          case TIMESTAMP:
            tuple.put(i, DatumFactory.createTimeStampFromMillis(buffer.getLong()));
            break;

          case BLOB : {
            //byte [] rawBytes = getColumnBytes();
            int byteSize = buffer.getInt();
            byte [] rawBytes = new byte[byteSize];
            buffer.get(rawBytes);
            tuple.put(i, DatumFactory.createBlob(rawBytes));
            break;
          }

          case PROTOBUF: {
            //byte [] rawBytes = getColumnBytes();
            int byteSize = buffer.getInt();
            byte [] rawBytes = new byte[byteSize];
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

          case NULL_TYPE:
            tuple.put(i, NullDatum.get());
            break;

          default:
        }
      }

      if(!buffer.hasRemaining() && channel.position() == fileSize){
        eof = true;
      }
      return tuple;
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
      buffer.clear();
      channel.close();
      fis.close();
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
  }

  public static class RawFileAppender extends FileAppender {
    private FileChannel channel;
    private RandomAccessFile randomAccessFile;
    private DataType[] columnTypes;

    private ByteBuffer buffer;
    private BitArray nullFlags;
    private int headerSize = 0;
    private static final int RECORD_SIZE = 4;
    private long pos;

    private TableStatistics stats;

    public RawFileAppender(Configuration conf, Schema schema, TableMeta meta, Path path) throws IOException {
      super(conf, schema, meta, path);
    }

    public void init() throws IOException {
      // TODO - RawFile only works on Local File System.
      //Preconditions.checkArgument(FileUtil.isLocalPath(path));
      File file = new File(path.toUri());
      randomAccessFile = new RandomAccessFile(file, "rw");
      channel = randomAccessFile.getChannel();
      pos = 0;

      columnTypes = new DataType[schema.getColumnNum()];
      for (int i = 0; i < schema.getColumnNum(); i++) {
        columnTypes[i] = schema.getColumn(i).getDataType();
      }

      buffer = ByteBuffer.allocateDirect(64 * 1024);

      // comput the number of bytes, representing the null flags

      nullFlags = new BitArray(schema.getColumnNum());
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

    @Override
    public void addTuple(Tuple t) throws IOException {

      if (buffer.remaining() < headerSize) {
        flushBuffer();
      }

      // skip the row header
      int recordOffset = buffer.position();
      buffer.position(recordOffset + headerSize);
      // reset the null flags
      nullFlags.clear();
      for (int i = 0; i < schema.getColumnNum(); i++) {
        if (enabledStats) {
          stats.analyzeField(i, t.get(i));
        }

        if (t.isNull(i)) {
          nullFlags.set(i);
          continue;
        }

        // 8 is the maximum bytes size of all types
        if (flushBufferAndReplace(recordOffset, 8)) {
          recordOffset = 0;
        }

        switch(columnTypes[i].getType()) {
          case NULL_TYPE:
            nullFlags.set(i);
            continue;

          case BOOLEAN:
          case BIT:
            buffer.put(t.get(i).asByte());
            break;

          case CHAR :
            byte[] src = t.getChar(i).asByteArray();
            byte[] dst = Arrays.copyOf(src, columnTypes[i].getLength());
            buffer.putInt(src.length);
            buffer.put(dst);
            break;

          case INT2 :
            buffer.putShort(t.get(i).asInt2());
            break;

          case INT4 :
            buffer.putInt(t.get(i).asInt4());
            break;

          case INT8 :
            buffer.putLong(t.get(i).asInt8());
            break;

          case FLOAT4 :
            buffer.putFloat(t.get(i).asFloat4());
            break;

          case FLOAT8 :
            buffer.putDouble(t.get(i).asFloat8());
            break;

          case TEXT:
            byte [] strBytes2 = t.get(i).asByteArray();
            if (flushBufferAndReplace(recordOffset, strBytes2.length + 4)) {
              recordOffset = 0;
            }
            buffer.putInt(strBytes2.length);
            buffer.put(strBytes2);
            break;

          case TIMESTAMP:
            buffer.putLong(((TimestampDatum)t.get(i)).getMillis());
            break;

          case BLOB : {
            byte [] rawBytes = t.get(i).asByteArray();
            if (flushBufferAndReplace(recordOffset, rawBytes.length + 4)) {
              recordOffset = 0;
            }
            buffer.putInt(rawBytes.length);
            buffer.put(rawBytes);
            break;
          }

          case PROTOBUF: {
            // TODO - to be fixed
//            byte [] lengthByte = new byte[4];
//            byte [] byteArray = t.get(i).asByteArray();
//            CodedOutputStream outputStream = CodedOutputStream.newInstance(lengthByte);
//            outputStream.writeUInt32NoTag(byteArray.length);
//            outputStream.flush();
//            int legnthByteLength = CodedOutputStream.computeInt32SizeNoTag(byteArray.length);
//            if (flushBufferAndReplace(recordOffset, byteArray.length + legnthByteLength)) {
//              recordOffset = 0;
//            }
//            buffer.put(lengthByte, 0, legnthByteLength);
            byte [] rawBytes = t.get(i).asByteArray();
            if (flushBufferAndReplace(recordOffset, rawBytes.length + 4)) {
              recordOffset = 0;
            }
            buffer.putInt(rawBytes.length);
            buffer.put(rawBytes);
            break;
          }

          case INET4 :
            buffer.put(t.get(i).asByteArray());
            break;

          default:
            throw new IOException("Cannot support data type: " + columnTypes[i].getType());
        }
      }

      // write a record header
      int bufferPos = buffer.position();
      buffer.position(recordOffset);
      buffer.putInt(bufferPos - recordOffset);
      byte [] flags = nullFlags.toArray();
      buffer.putShort((short) flags.length);
      buffer.put(flags);

      pos += bufferPos - recordOffset;
      buffer.position(bufferPos);

      if (enabledStats) {
        stats.incrementRow();
      }
    }

    @Override
    public void flush() throws IOException {
      flushBuffer();
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
      channel.close();
      randomAccessFile.close();
    }

    @Override
    public TableStats getStats() {
      if (enabledStats) {
        return stats.getTableStat();
      } else {
        return null;
      }
    }
  }
}
