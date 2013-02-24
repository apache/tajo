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

package tajo.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.catalog.Column;
import tajo.catalog.TableMeta;
import tajo.catalog.statistics.TableStat;
import tajo.conf.TajoConf.ConfVars;
import tajo.datum.ArrayDatum;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.datum.json.GsonCreator;
import tajo.storage.exception.AlreadyExistsStorageException;
import tajo.util.BitArray;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class RowFile {
  public static final Log LOG = LogFactory.getLog(RowFile.class);

  private static final int SYNC_ESCAPE = -1;
  private static final int SYNC_HASH_SIZE = 16;
  private static final int SYNC_SIZE = 4 + SYNC_HASH_SIZE;
  private final static int DEFAULT_BUFFER_SIZE = 65535;
  public static int SYNC_INTERVAL;

  public static class RowFileScanner extends FileScanner {
    private FileSystem fs;
    private FSDataInputStream in;
    private Tuple tuple;

    private byte[] sync = new byte[SYNC_HASH_SIZE];
    private byte[] checkSync = new byte[SYNC_HASH_SIZE];
    private long start, end;

    private ByteBuffer buffer;
    private final int tupleHeaderSize;
    private BitArray nullFlags;
    private int numBitsOfNullFlags;
    private long bufferStartPos;

    public RowFileScanner(Configuration conf, final TableMeta meta,
                          final Fragment fragment) throws IOException {
      super(conf, meta, fragment);

      SYNC_INTERVAL =
          conf.getInt(ConfVars.RAWFILE_SYNC_INTERVAL.varname,
              SYNC_SIZE * 100);
      numBitsOfNullFlags = (int) Math.ceil(((double)schema.getColumnNum()));
      nullFlags = new BitArray(numBitsOfNullFlags);
      tupleHeaderSize = nullFlags.size() + (2 * Short.SIZE/8);
      this.start = fragment.getStartOffset();
      this.end = this.start + fragment.getLength();

      init();
    }

    public void init() throws IOException {
      // set default page size.
      fs = fragment.getPath().getFileSystem(conf);
      in = fs.open(fragment.getPath());
      buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE * schema.getColumnNum());
      buffer.flip();

      readHeader();

      // find the correct position from the start
      if (this.start > in.getPos()) {
        long realStart = start > SYNC_SIZE ? (start-SYNC_SIZE) : 0;
        in.seek(realStart);
      }
      bufferStartPos = in.getPos();
      fillBuffer();
      fillBuffer(); // due to the bug of FSDataInputStream.read(ByteBuffer)

      if (start != 0) {
        // TODO: improve
        boolean syncFound = false;
        while (!syncFound) {
          buffer.mark();
          syncFound = checkSync();
          if (!syncFound) {
            buffer.reset();
            buffer.get(); // proceed one byte
          }
        }
        bufferStartPos += buffer.position();
        buffer.compact();
        buffer.flip();
      }

      super.init();
    }

    private void readHeader() throws IOException {
      SYNC_INTERVAL = in.readInt();
      in.read(this.sync, 0, SYNC_HASH_SIZE);
    }

    /**
     * Find the sync from the front of the buffer
     *
     * @return return true if it succeeds to find the sync.
     * @throws IOException
     */
    private boolean checkSync() throws IOException {
      buffer.getInt();                           // escape
      buffer.get(checkSync, 0, SYNC_HASH_SIZE);  // sync
      return Arrays.equals(checkSync, sync);
    }

    private boolean fillBuffer() throws IOException {
      bufferStartPos += buffer.position();
      buffer.compact();
      int read = in.read(buffer);
      if (read < 0) {
        return false;
      } else {
        buffer.flip();
        return true;
      }
    }

    @Override
    public Tuple next() throws IOException {
      while (buffer.remaining() < SYNC_SIZE) {
        if (!fillBuffer()) {
          return null;
        }
      }

      buffer.mark();
      if (!checkSync()) {
        buffer.reset();
      } else {
        if (bufferStartPos + buffer.position() > end) {
          return null;
        }
      }

      while (buffer.remaining() < tupleHeaderSize) {
        if (!fillBuffer()) {
          return null;
        }
      }

      int i;
      tuple = new VTuple(schema.getColumnNum());

      int nullFlagSize = buffer.getShort();
      byte[] nullFlagBytes = new byte[nullFlagSize];
      buffer.get(nullFlagBytes, 0, nullFlagSize);
      nullFlags = new BitArray(nullFlagBytes);
      int tupleSize = buffer.getShort();

      while (buffer.remaining() < (tupleSize)) {
        if (!fillBuffer()) {
          return null;
        }
      }

      Datum datum;
      Column col;
      for (i = 0; i < schema.getColumnNum(); i++) {
        if (!nullFlags.get(i)) {
          col = schema.getColumn(i);
          switch (col.getDataType()) {
            case BOOLEAN:
              datum = DatumFactory.createBool(buffer.get());
              tuple.put(i, datum);
              break;

            case BYTE:
              datum = DatumFactory.createByte(buffer.get());
              tuple.put(i, datum );
              break;

            case CHAR:
              datum = DatumFactory.createChar(buffer.getChar());
              tuple.put(i, datum);
              break;

            case SHORT:
              datum = DatumFactory.createShort(buffer.getShort());
              tuple.put(i, datum );
              break;

            case INT:
              datum = DatumFactory.createInt(buffer.getInt());
              tuple.put(i, datum );
              break;

            case LONG:
              datum = DatumFactory.createLong(buffer.getLong());
              tuple.put(i, datum );
              break;

            case FLOAT:
              datum = DatumFactory.createFloat(buffer.getFloat());
              tuple.put(i, datum);
              break;

            case DOUBLE:
              datum = DatumFactory.createDouble(buffer.getDouble());
              tuple.put(i, datum);
              break;

            case STRING:
              short len = buffer.getShort();
              byte[] buf = new byte[len];
              buffer.get(buf, 0, len);
              datum = DatumFactory.createString(buf);
              tuple.put(i, datum);
              break;

            case STRING2:
              short bytelen = buffer.getShort();
              byte[] strbytes = new byte[bytelen];
              buffer.get(strbytes, 0, bytelen);
              datum = DatumFactory.createString2(strbytes);
              tuple.put(i, datum);
              break;

            case BYTES:
              short bytesLen = buffer.getShort();
              byte [] bytesBuf = new byte[bytesLen];
              buffer.get(bytesBuf);
              datum = DatumFactory.createBytes(bytesBuf);
              tuple.put(i, datum);
              break;

            case IPv4:
              byte[] ipv4 = new byte[4];
              buffer.get(ipv4, 0, 4);
              datum = DatumFactory.createIPv4(ipv4);
              tuple.put(i, datum);
              break;

            case ARRAY:
              short bufSize = buffer.getShort();
              byte [] bytes = new byte[bufSize];
              buffer.get(bytes);
              String json = new String(bytes);
              ArrayDatum array = (ArrayDatum) GsonCreator.getInstance().fromJson(json, Datum.class);
              tuple.put(i, array);
              break;

            default:
              break;
          }
        } else {
          tuple.put(i, DatumFactory.createNullDatum());
        }
      }
      return tuple;
    }

    @Override
    public void reset() throws IOException {
      init();
    }

    @Override
    public void close() throws IOException {
      if (in != null) {
        in.close();
      }
    }

    @Override
    public boolean isProjectable() {
      return false;
    }

    @Override
    public boolean isSelectable() {
      return false;
    }
  }

  public static class RowFileAppender extends FileAppender {
    private FSDataOutputStream out;
    private long lastSyncPos;
    private FileSystem fs;
    private byte[] sync;
    private ByteBuffer buffer;

    private BitArray nullFlags;
    private int numBitsOfNullFlags;

    // statistics
    private TableStatistics stats;

    public RowFileAppender(Configuration conf, final TableMeta meta, final Path path)
        throws IOException {
      super(conf, meta, path);
    }

    public void init() throws IOException {
      SYNC_INTERVAL = conf.getInt(ConfVars.RAWFILE_SYNC_INTERVAL.varname, 100);

      fs = path.getFileSystem(conf);

      if (!fs.exists(path.getParent())) {
        throw new FileNotFoundException(path.toString());
      }

      if (fs.exists(path)) {
        throw new AlreadyExistsStorageException(path);
      }

      sync = new byte[SYNC_HASH_SIZE];
      lastSyncPos = 0;

      out = fs.create(path);

      MessageDigest md;
      try {
        md = MessageDigest.getInstance("MD5");
        md.update((path.toString()+System.currentTimeMillis()).getBytes());
        sync = md.digest();
      } catch (NoSuchAlgorithmException e) {
        LOG.error(e);
      }

      writeHeader();

      buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);

      numBitsOfNullFlags = (int) Math.ceil(((double)schema.getColumnNum()));
      nullFlags = new BitArray(numBitsOfNullFlags);

      if (enabledStats) {
        this.stats = new TableStatistics(this.schema);
      }
    }

    private void writeHeader() throws IOException {
      out.writeInt(SYNC_INTERVAL);
      out.write(sync);
      out.flush();
      lastSyncPos = out.getPos();
    }

    @Override
    public void addTuple(Tuple t) throws IOException {
      checkAndWriteSync();
      Column col;

      buffer.clear();
      nullFlags.clear();

      for (int i = 0; i < schema.getColumnNum(); i++) {
        if (enabledStats) {
          stats.analyzeField(i, t.get(i));
        }

        if (t.isNull(i)) {
          nullFlags.set(i);
        } else {
          col = schema.getColumn(i);
          switch (col.getDataType()) {
            case BOOLEAN:
//              out.writeBoolean(t.getByte(i).asBool());
              buffer.put(t.getBoolean(i).asByte());
              break;
            case BYTE:
//              out.writeByte(t.getByte(i).asByte());
              buffer.put(t.getByte(i).asByte());
              break;
            case CHAR:
//              out.writeChar(t.getChar(i).asChar());
              buffer.putChar(t.getChar(i).asChar());
              break;
            case STRING:
              byte[] buf = t.getString(i).asByteArray();
              if (buf.length > 256) {
                buf = new byte[256];
                byte[] str = t.getString(i).asByteArray();
                System.arraycopy(str, 0, buf, 0, 256);
              }
//              out.writeShort(buf.length);
//              out.write(buf, 0, buf.length);
              buffer.putShort((short)buf.length);
              buffer.put(buf, 0, buf.length);
              break;
            case STRING2:
              byte[] strbytes = t.getString2(i).asByteArray();
              buffer.putShort((short)strbytes.length);
              buffer.put(strbytes, 0, strbytes.length);
              break;
            case SHORT:
//              out.writeShort(t.getShort(i).asShort());
              buffer.putShort(t.getShort(i).asShort());
              break;
            case INT:
//              out.writeInt(t.getInt(i).asInt());
              buffer.putInt(t.getInt(i).asInt());
              break;
            case LONG:
//              out.writeLong(t.getLong(i).asLong());
              buffer.putLong(t.getLong(i).asLong());
              break;
            case FLOAT:
//              out.writeFloat(t.getFloat(i).asFloat());
              buffer.putFloat(t.getFloat(i).asFloat());
              break;
            case DOUBLE:
//              out.writeDouble(t.getDouble(i).asDouble());
              buffer.putDouble(t.getDouble(i).asDouble());
              break;
            case BYTES:
              byte [] bytes = t.getBytes(i).asByteArray();
//              out.writeInt(bytes.length);
//              out.write(bytes);
              buffer.putShort((short)bytes.length);
              buffer.put(bytes);
              break;
            case IPv4:
//              out.write(t.getIPv4Bytes(i));
              buffer.put(t.getIPv4Bytes(i));
              break;
            case IPv6:
//              out.write(t.getIPv6Bytes(i));
              buffer.put(t.getIPv6Bytes(i));
            case ARRAY: {
              ArrayDatum array = (ArrayDatum) t.get(i);
              String json = array.toJSON();
              byte [] byteArray = json.getBytes();
//              out.writeInt(byteArray.length);
//              out.write(byteArray);
              buffer.putShort((short)byteArray.length);
              buffer.put(byteArray);
              break;
            }
            default:
              break;
          }
        }
      }

      byte[] bytes = nullFlags.toArray();
      out.writeShort(bytes.length);
      out.write(bytes);

      bytes = buffer.array();
      int dataLen = buffer.position();
      out.writeShort(dataLen);
      out.write(bytes, 0, dataLen);

      // Statistical section
      if (enabledStats) {
        stats.incrementRow();
      }
    }

    @Override
    public long getOffset() throws IOException {
      return out.getPos();
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }

    @Override
    public void close() throws IOException {
      if (out != null) {
        if (enabledStats) {
          stats.setNumBytes(out.getPos());
        }
        sync();
        out.flush();
        out.close();
      }
    }

    private void sync() throws IOException {
      if (lastSyncPos != out.getPos()) {
        out.writeInt(SYNC_ESCAPE);
        out.write(sync);
        lastSyncPos = out.getPos();
      }
    }

    synchronized void checkAndWriteSync() throws IOException {
      if (out.getPos() >= lastSyncPos + SYNC_INTERVAL) {
        sync();
      }
    }

    @Override
    public TableStat getStats() {
      if (enabledStats) {
        return stats.getTableStat();
      } else {
        return null;
      }
    }
  }
}
