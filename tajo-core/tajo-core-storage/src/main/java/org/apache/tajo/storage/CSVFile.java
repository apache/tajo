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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.datum.*;
import org.apache.tajo.storage.exception.AlreadyExistsStorageException;
import org.apache.tajo.storage.json.StorageGsonHelper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

public class CSVFile {
  public static final String DELIMITER = "csvfile.delimiter";
  public static final String DELIMITER_DEFAULT = "|";
  private static final Log LOG = LogFactory.getLog(CSVFile.class);

  public static class CSVAppender extends FileAppender {
    private final TableMeta meta;
    private final Schema schema;
    private final FileSystem fs;
    private FSDataOutputStream fos;
    private String delimiter;
    private TableStatistics stats = null;

    public CSVAppender(Configuration conf, final TableMeta meta,
        final Path path) throws IOException {
      super(conf, meta, path);
      this.fs = path.getFileSystem(conf);
      this.meta = meta;
      this.schema = meta.getSchema();
      this.delimiter = this.meta.getOption(DELIMITER, DELIMITER_DEFAULT);
    }

    @Override
    public void init() throws IOException {
      if (!fs.exists(path.getParent())) {
        throw new FileNotFoundException(path.toString());
      }

      if (fs.exists(path)) {
        throw new AlreadyExistsStorageException(path);
      }

      fos = fs.create(path);

      if (enabledStats) {
        this.stats = new TableStatistics(this.schema);
      }

      super.init();
    }

    @Override
    public void addTuple(Tuple tuple) throws IOException {
      StringBuilder sb = new StringBuilder();
      Column col;
      Datum datum;
      for (int i = 0; i < schema.getColumnNum(); i++) {
        datum = tuple.get(i);
        if (enabledStats) {
          stats.analyzeField(i, datum);
        }
        if (datum instanceof NullDatum) {
        } else {
          col = schema.getColumn(i);
          switch (col.getDataType().getType()) {
          case BOOLEAN:
            sb.append(tuple.getBoolean(i));
            break;
          case BIT:
            sb.append(new String(Base64.encodeBase64(tuple.getByte(i)
                .asByteArray(), false)));
            break;
          case BLOB:
            sb.append(new String(Base64.encodeBase64(tuple.getBytes(i)
                .asByteArray(), false)));
            break;
          case CHAR:
            sb.append(tuple.getChar(i));
            break;
//          case STRING:
//            sb.append(tuple.getString(i));
//            break;
          case TEXT:
            TextDatum td = tuple.getText(i);
            sb.append(td.toString());
            break;
          case INT2:
            sb.append(tuple.getShort(i));
            break;
          case INT4:
            sb.append(tuple.getInt(i));
            break;
          case INT8:
            sb.append(tuple.getLong(i));
            break;
          case FLOAT4:
            sb.append(tuple.getFloat(i));
            break;
          case FLOAT8:
            sb.append(tuple.getDouble(i));
            break;
          case INET4:
            sb.append(tuple.getIPv4(i));
            break;
          case INET6:
            sb.append(tuple.getIPv6(i));
          case ARRAY:
            /*
             * sb.append("["); boolean first = true; ArrayDatum array =
             * (ArrayDatum) tuple.get(i); for (Datum field : array.toArray()) {
             * if (first) { first = false; } else { sb.append(delimiter); }
             * sb.append(field.asChars()); } sb.append("]");
             */
            ArrayDatum array = (ArrayDatum) tuple.get(i);
            sb.append(array.toJson());
            break;
          default:
            throw new UnsupportedOperationException("Cannot write such field: "
                + tuple.get(i).type());
          }
        }
        sb.append(delimiter);
      }
      if (sb.length() > 0) {
        sb.deleteCharAt(sb.length() - 1);
      }
      sb.append('\n');
      fos.writeBytes(sb.toString());

      // Statistical section
      if (enabledStats) {
        stats.incrementRow();
      }
    }

    @Override
    public long getOffset() throws IOException {
      return fos.getPos();
    }

    @Override
    public void flush() throws IOException {
      fos.flush();
    }

    @Override
    public void close() throws IOException {
      // Statistical section
      if (enabledStats) {
        stats.setNumBytes(fos.getPos());
      }
      fos.close();
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

  public static class CSVScanner extends FileScanner implements SeekableScanner {
    public CSVScanner(Configuration conf, final TableMeta meta,
        final Fragment fragment) throws IOException {
      super(conf, meta, fragment);
    }

    private static final byte LF = '\n';
    private final static long DEFAULT_BUFFER_SIZE = 256 * 1024;
    private long bufSize;
    private char delimiter;
    private FileSystem fs;
    private FSDataInputStream fis;
    private long startOffset, length, startPos;
    private byte[] buf = null;
    private String[] tuples = null;
    private long[] tupleOffsets = null;
    private int currentIdx = 0, validIdx = 0;
    private byte[] tail = null;
    private long pageStart = -1;
    private long prevTailLen = -1;
    private int[] targetColumnIndexes;

    @Override
    public void init() throws IOException {

      // Buffer size, Delimiter
      this.bufSize = DEFAULT_BUFFER_SIZE;
      String delim  = fragment.getMeta().getOption(DELIMITER, DELIMITER_DEFAULT);
      this.delimiter = delim.charAt(0);

      // Fragment information
      this.fs = fragment.getPath().getFileSystem(this.conf);
      this.fis = this.fs.open(fragment.getPath());
      this.startOffset = fragment.getStartOffset();
      this.length = fragment.getLength();
      tuples = new String[0];

      if (targets == null) {
        targets = schema.toArray();
      }

      targetColumnIndexes = new int[targets.length];
      for (int i = 0; i < targets.length; i++) {
        targetColumnIndexes[i] = schema.getColumnIdByName(targets[i].getColumnName());
      }
      super.init();

      if (startOffset != 0) {
        fis.seek(startOffset - 1);
        while (fis.readByte() != LF) {
        }
      }
      startPos = fis.getPos();
      if (fragmentable() < 1) {
        fis.close();
        return;
      }
      page();
    }

    private long fragmentable() throws IOException {
      return startOffset + length - fis.getPos();
    }

    private void page() throws IOException {
      // Index initialization
      currentIdx = 0;

      // Buffer size set
      if (fragmentable() < DEFAULT_BUFFER_SIZE) {
        bufSize = fragmentable();
      }


      if (this.tail == null || this.tail.length == 0) {
        this.pageStart = fis.getPos();
        this.prevTailLen = 0;
      } else {
        this.pageStart = fis.getPos() - this.tail.length;
        this.prevTailLen = this.tail.length;
      }

      // Read
      int rbyte;
      if (fis.getPos() == startPos) {
        buf = new byte[(int) bufSize];
        rbyte = fis.read(buf);
        tail = new byte[0];
        tuples = StringUtils.split(new String(buf, 0, rbyte), (char)LF);
      } else {
        buf = new byte[(int) bufSize];
        rbyte = fis.read(buf);
        tuples = StringUtils.split(new String(tail) + new String(buf, 0, rbyte), (char)LF);
      }

      // Check tail
      if ((char) buf[rbyte - 1] != LF) {
        if (fragmentable() < 1) {
          int cnt = 0;
          byte[] temp = new byte[(int)DEFAULT_BUFFER_SIZE];
          // Read bytes
          while ((temp[cnt] = fis.readByte()) != LF) {
            cnt++;
          }

          // Replace tuple
          tuples[tuples.length - 1] = tuples[tuples.length - 1] + new String(temp, 0, cnt);
          validIdx = tuples.length;
        } else {
          tail = tuples[tuples.length - 1].getBytes();
          validIdx = tuples.length - 1;
        }
      } else {
        tail = new byte[0];
        validIdx = tuples.length;
      }
      makeTupleOffset();
    }

    private void makeTupleOffset() {
      long curTupleOffset = 0;
      this.tupleOffsets = new long[this.validIdx];
      for (int i = 0; i < this.validIdx; i++) {
        this.tupleOffsets[i] = curTupleOffset + this.pageStart;
        curTupleOffset += this.tuples[i].getBytes().length + 1;//tuple byte +  1byte line feed
      }
      
    }

    @Override
    public Tuple next() throws IOException {
      try {
        if (currentIdx == validIdx) {
          if (fragmentable() < 1) {
            fis.close();
            return null;
          } else {
            page();
          }
        }
        long offset = this.tupleOffsets[currentIdx];
        String[] cells = StringUtils.splitPreserveAllTokens(tuples[currentIdx++], delimiter);
        int targetLen = targets.length;
        VTuple tuple = new VTuple(columnNum);
        Column field;
        tuple.setOffset(offset);
        for (int i = 0; i < targetLen; i++) {
          field = targets[i];
          int tid = targetColumnIndexes[i];
          if (cells.length <= tid) {
            tuple.put(tid, DatumFactory.createNullDatum());
          } else {
            String cell = cells[tid].trim();

            if (cell.equals("")) {
              tuple.put(tid, DatumFactory.createNullDatum());
            } else {
              switch (field.getDataType().getType()) {
              case BOOLEAN:
                tuple.put(tid, DatumFactory.createBool(cell));
                break;
              case BIT:
                tuple.put(tid, DatumFactory.createBit(Base64.decodeBase64(cell)[0]));
                break;
              case CHAR:
                tuple.put(tid, DatumFactory.createChar(cell.charAt(0)));
                break;
              case BLOB:
                tuple.put(tid, DatumFactory.createBlob(Base64.decodeBase64(cell)));
                break;
              case INT2:
                tuple.put(tid, DatumFactory.createInt2(cell));
                break;
              case INT4:
                tuple.put(tid, DatumFactory.createInt4(cell));
                break;
              case INT8:
                tuple.put(tid, DatumFactory.createInt8(cell));
                break;
              case FLOAT4:
                tuple.put(tid, DatumFactory.createFloat4(cell));
                break;
              case FLOAT8:
                tuple.put(tid, DatumFactory.createFloat8(cell));
                break;
              case TEXT:
                tuple.put(tid, DatumFactory.createText(cell));
                break;
              case INET4:
                tuple.put(tid, DatumFactory.createInet4(cell));
                break;
              case ARRAY:
                Datum data = StorageGsonHelper.getInstance().fromJson(cell,
                    Datum.class);
                tuple.put(tid, data);
                break;
              }
            }
          }
        }
        return tuple;
      } catch (Throwable t) {
        LOG.error("Tuple list length: " + tuples.length, t);
        LOG.error("Tuple list current index: " + currentIdx, t);
      }
      return null;
    }

    @Override
    public void reset() throws IOException {
      init();
    }

    @Override
    public void close() throws IOException {
      fis.close();
    }

    @Override
    public boolean isProjectable() {
      return true;
    }

    @Override
    public boolean isSelectable() {
      return false;
    }

    @Override
    public void setSearchCondition(Object expr) {
    }

    @Override
    public void seek(long offset) throws IOException {
      int tupleIndex = Arrays.binarySearch(this.tupleOffsets, offset);
      if (tupleIndex > -1) {
        this.currentIdx = tupleIndex;
      } else if (offset >= this.pageStart + this.bufSize 
          + this.prevTailLen - this.tail.length || offset <= this.pageStart) {
        fis.seek(offset);
        tail = new byte[0];
        buf = new byte[(int) DEFAULT_BUFFER_SIZE];
        bufSize = DEFAULT_BUFFER_SIZE;
        this.currentIdx = 0;
        this.validIdx = 0;
        // pageBuffer();
      } else {
        throw new IOException("invalid offset " +
           " < pageStart : " +  this.pageStart + " , " + 
           "  pagelength : " + this.bufSize + " , " + 
           "  tail lenght : " + this.tail.length +
           "  input offset : " + offset + " >");
      }

    }

    @Override
    public long getNextOffset() throws IOException {
      if (this.currentIdx == this.validIdx) {
        if (fragmentable() < 1) {
          return -1;
        } else {
          page();
        }
      }
      return this.tupleOffsets[currentIdx];
    }

    @Override
    public boolean isSplittable(){
      return true;
    }
  }
}
