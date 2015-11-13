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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.serder.PlanProto.ShuffleType;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.exception.AlreadyExistsStorageException;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.BitArray;

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
    private long bufferStartPos;

    public RowFileScanner(Configuration conf, final Schema schema, final TableMeta meta, final Fragment fragment)
        throws IOException {
      super(conf, schema, meta, fragment);

      SYNC_INTERVAL = conf.getInt(ConfVars.ROWFILE_SYNC_INTERVAL.varname,
          ConfVars.ROWFILE_SYNC_INTERVAL.defaultIntVal) * SYNC_SIZE;

      nullFlags = new BitArray(schema.size());
      tupleHeaderSize = nullFlags.bytesLength() + (2 * Short.SIZE / 8);
      this.start = this.fragment.getStartKey();
      this.end = this.start + this.fragment.getLength();
    }

    public void init() throws IOException {
      // set default page size.
      fs = fragment.getPath().getFileSystem(conf);
      in = fs.open(fragment.getPath());
      buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE * schema.size());
      buffer.flip();

      readHeader();

      // find the correct position from the start
      if (this.start > in.getPos()) {
        long realStart = start > SYNC_SIZE ? (start-SYNC_SIZE) : 0;
        in.seek(realStart);
      }
      bufferStartPos = in.getPos();
      fillBuffer();

      if (start != 0) {
        // TODO: improve
        boolean syncFound = false;
        while (!syncFound) {
          if (buffer.remaining() < SYNC_SIZE) {
            fillBuffer();
          }
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

      tuple = new VTuple(schema.size());

      super.init();
    }

    private void readHeader() throws IOException {
      SYNC_INTERVAL = in.readInt();
      StorageUtil.readFully(in, this.sync, 0, SYNC_HASH_SIZE);
    }

    /**
     * Find the sync from the front of the buffer
     *
     * @return return true if it succeeds to find the sync.
     * @throws java.io.IOException
     */
    private boolean checkSync() throws IOException {
      buffer.getInt();                           // escape
      buffer.get(checkSync, 0, SYNC_HASH_SIZE);  // sync
      return Arrays.equals(checkSync, sync);
    }

    private int fillBuffer() throws IOException {
      bufferStartPos += buffer.position();
      buffer.compact();
      int remain = buffer.remaining();
      int read = in.read(buffer);
      if (read == -1) {
        buffer.flip();
        return read;
      } else {
        int totalRead = read;
        if (remain > totalRead) {
          read = in.read(buffer);
          totalRead += read > 0 ? read : 0;
        }
        buffer.flip();
        return totalRead;
      }
    }

    @Override
    public Tuple next() throws IOException {
      while (buffer.remaining() < SYNC_SIZE) {
        if (fillBuffer() < 0) {
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
        if (fillBuffer() < 0) {
          return null;
        }
      }

      int i;

      int nullFlagSize = buffer.getShort();
      byte[] nullFlagBytes = new byte[nullFlagSize];
      buffer.get(nullFlagBytes, 0, nullFlagSize);
      nullFlags = new BitArray(nullFlagBytes);
      int tupleSize = buffer.getShort();

      while (buffer.remaining() < (tupleSize)) {
        if (fillBuffer() < 0) {
          return null;
        }
      }

      Datum datum;
      Column col;
      for (i = 0; i < schema.size(); i++) {
        if (!nullFlags.get(i)) {
          col = schema.getColumn(i);
          switch (col.getDataType().getType()) {
            case BOOLEAN :
              datum = DatumFactory.createBool(buffer.get());
              tuple.put(i, datum);
              break;

            case BIT:
              datum = DatumFactory.createBit(buffer.get());
              tuple.put(i, datum );
              break;

            case CHAR :
              int realLen = buffer.getInt();
              byte[] buf = new byte[col.getDataType().getLength()];
              buffer.get(buf);
              byte[] charBuf = Arrays.copyOf(buf, realLen);
              tuple.put(i, DatumFactory.createChar(charBuf));
              break;

            case INT2 :
              datum = DatumFactory.createInt2(buffer.getShort());
              tuple.put(i, datum );
              break;

            case INT4 :
              datum = DatumFactory.createInt4(buffer.getInt());
              tuple.put(i, datum );
              break;

            case INT8 :
              datum = DatumFactory.createInt8(buffer.getLong());
              tuple.put(i, datum );
              break;

            case FLOAT4 :
              datum = DatumFactory.createFloat4(buffer.getFloat());
              tuple.put(i, datum);
              break;

            case FLOAT8 :
              datum = DatumFactory.createFloat8(buffer.getDouble());
              tuple.put(i, datum);
              break;

            case TEXT:
              short bytelen = buffer.getShort();
              byte[] strbytes = new byte[bytelen];
              buffer.get(strbytes, 0, bytelen);
              datum = DatumFactory.createText(strbytes);
              tuple.put(i, datum);
              break;

            case BLOB:
              short bytesLen = buffer.getShort();
              byte [] bytesBuf = new byte[bytesLen];
              buffer.get(bytesBuf);
              datum = DatumFactory.createBlob(bytesBuf);
              tuple.put(i, datum);
              break;

            case INET4 :
              byte[] ipv4 = new byte[4];
              buffer.get(ipv4, 0, 4);
              datum = DatumFactory.createInet4(ipv4);
              tuple.put(i, datum);
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

    @Override
    public void setFilter(EvalNode filter) {
      throw new TajoRuntimeException(new UnsupportedException());
    }

    @Override
    public boolean isSplittable(){
      return true;
    }
  }

  public static class RowFileAppender extends FileAppender {
    private FSDataOutputStream out;
    private long lastSyncPos;
    private FileSystem fs;
    private byte[] sync;
    private ByteBuffer buffer;

    private BitArray nullFlags;
    // statistics
    private TableStatistics stats;
    private ShuffleType shuffleType;

    public RowFileAppender(Configuration conf, final TaskAttemptId taskAttemptId,
                           final Schema schema, final TableMeta meta, final Path workDir)
        throws IOException {
      super(conf, taskAttemptId, schema, meta, workDir);
    }

    public void init() throws IOException {
      SYNC_INTERVAL = conf.getInt(ConfVars.ROWFILE_SYNC_INTERVAL.varname,
          ConfVars.ROWFILE_SYNC_INTERVAL.defaultIntVal);
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

      nullFlags = new BitArray(schema.size());

      if (tableStatsEnabled) {
        this.stats = new TableStatistics(this.schema, columnStatsEnabled);
        this.shuffleType = PlannerUtil.getShuffleType(
            meta.getOption(StorageConstants.SHUFFLE_TYPE,
                PlannerUtil.getShuffleType(ShuffleType.NONE_SHUFFLE)));
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

      for (int i = 0; i < schema.size(); i++) {
        if (shuffleType == ShuffleType.RANGE_SHUFFLE) {
          // it is to calculate min/max values, and it is only used for the intermediate file.
          stats.analyzeField(i, t);
        }

        if (t.isBlankOrNull(i)) {
          nullFlags.set(i);
        } else {
          col = schema.getColumn(i);
          switch (col.getDataType().getType()) {
            case BOOLEAN:
              buffer.put(t.getByte(i));
              break;
            case BIT:
              buffer.put(t.getByte(i));
              break;
            case CHAR:
              byte[] src = t.getBytes(i);
              byte[] dst = Arrays.copyOf(src, col.getDataType().getLength());
              buffer.putInt(src.length);
              buffer.put(dst);
              break;
            case TEXT:
              byte [] strbytes = t.getBytes(i);
              buffer.putShort((short)strbytes.length);
              buffer.put(strbytes, 0, strbytes.length);
              break;
            case INT2:
              buffer.putShort(t.getInt2(i));
              break;
            case INT4:
              buffer.putInt(t.getInt4(i));
              break;
            case INT8:
              buffer.putLong(t.getInt8(i));
              break;
            case FLOAT4:
              buffer.putFloat(t.getFloat4(i));
              break;
            case FLOAT8:
              buffer.putDouble(t.getFloat8(i));
              break;
            case BLOB:
              byte [] bytes = t.getBytes(i);
              buffer.putShort((short)bytes.length);
              buffer.put(bytes);
              break;
            case INET4:
              buffer.put(t.getBytes(i));
              break;
            case INET6:
              buffer.put(t.getBytes(i));
              break;
            case NULL_TYPE:
              nullFlags.set(i);
              break;
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
      if (tableStatsEnabled) {
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
        if (tableStatsEnabled) {
          stats.setNumBytes(out.getPos());
        }
        sync();
        out.flush();
        IOUtils.cleanup(LOG, out);
      }
    }

    private void sync() throws IOException {
      if (lastSyncPos != out.getPos()) {
        out.writeInt(SYNC_ESCAPE);
        out.write(sync);
        lastSyncPos = out.getPos();
      }
    }

    private void checkAndWriteSync() throws IOException {
      if (out.getPos() >= lastSyncPos + SYNC_INTERVAL) {
        sync();
      }
    }

    @Override
    public TableStats getStats() {
      if (tableStatsEnabled) {
        return stats.getTableStat();
      } else {
        return null;
      }
    }
  }
}
