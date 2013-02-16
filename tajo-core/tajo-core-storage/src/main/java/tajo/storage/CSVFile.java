/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package tajo.storage;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.TableMeta;
import tajo.catalog.statistics.TableStat;
import tajo.datum.ArrayDatum;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.datum.DatumType;
import tajo.storage.exception.AlreadyExistsStorageException;
import tajo.storage.json.GsonCreator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

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
        if (datum.type() == DatumType.NULL) {
        } else {
          col = schema.getColumn(i);
          switch (col.getDataType()) {
          case BOOLEAN:
            sb.append(tuple.getBoolean(i));
            break;
          case BYTE:
            sb.append(new String(Base64.encodeBase64(tuple.getByte(i)
                .asByteArray(), false)));
            break;
          case BYTES:
            sb.append(new String(Base64.encodeBase64(tuple.getBytes(i)
                .asByteArray(), false)));
            break;
          case CHAR:
            sb.append(tuple.getChar(i));
            break;
          case STRING:
            sb.append(tuple.getString(i));
            break;
          case STRING2:
            sb.append(tuple.getString2(i));
            break;
          case SHORT:
            sb.append(tuple.getShort(i));
            break;
          case INT:
            sb.append(tuple.getInt(i));
            break;
          case LONG:
            sb.append(tuple.getLong(i));
            break;
          case FLOAT:
            sb.append(tuple.getFloat(i));
            break;
          case DOUBLE:
            sb.append(tuple.getDouble(i));
            break;
          case IPv4:
            sb.append(tuple.getIPv4(i));
            break;
          case IPv6:
            sb.append(tuple.getIPv6(i));
          case ARRAY:
            /*
             * sb.append("["); boolean first = true; ArrayDatum array =
             * (ArrayDatum) tuple.get(i); for (Datum field : array.toArray()) {
             * if (first) { first = false; } else { sb.append(delimiter); }
             * sb.append(field.asChars()); } sb.append("]");
             */
            ArrayDatum array = (ArrayDatum) tuple.get(i);
            sb.append(array.toJSON());
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
      init(fragment);
    }

    private static final byte LF = '\n';
    private final static long DEFAULT_BUFFER_SIZE = 65536;
    private long bufSize;
    private String delimiter;
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
    private HashMap<Long, Integer> curTupleOffsetMap = null;

    private void init(final Fragment fragment) throws IOException {

      // Buffer size, Delimiter
      this.bufSize = DEFAULT_BUFFER_SIZE;
      this.delimiter = fragment.getMeta().getOption(DELIMITER,
          DELIMITER_DEFAULT);
      if (this.delimiter.equals("|")) {
        this.delimiter = "\\|";
      }

      // Fragment information
      this.fs = fragment.getPath().getFileSystem(this.conf);
      this.fis = this.fs.open(fragment.getPath());
      this.startOffset = fragment.getStartOffset();
      this.length = fragment.getLength();
      tuples = new String[0];

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
        tuples = new String(buf,0,rbyte).split("\n");
      } else {
        buf = new byte[(int) bufSize];
        rbyte = fis.read(buf);
        tuples = (new String(tail) + new String(buf,0,rbyte)).split("\n");
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
          tuples[tuples.length - 1] = new String(tuples[tuples.length - 1]
              + new String(temp,0,cnt));
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
      this.tupleOffsets = null;
      this.tupleOffsets = new long[this.validIdx];
      
      this.curTupleOffsetMap = null;
      this.curTupleOffsetMap = new HashMap<>();
      
      for (int i = 0; i < this.validIdx; i++) {
        this.tupleOffsets[i] = curTupleOffset + this.pageStart;
        this.curTupleOffsetMap.put(tupleOffsets[i], i);
        curTupleOffset += (this.tuples[i]  + "\n").getBytes().length;
      }
      
    }

    @Override
    public void init() throws IOException {
      super.init();
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
        String[] cells = tuples[currentIdx++].split(delimiter);
        VTuple tuple = new VTuple(schema.getColumnNum());
        Column field;
        tuple.setOffset(offset);
        for (int i = 0; i < schema.getColumnNum(); i++) {
          field = schema.getColumn(i);
          if (cells.length <= i) {
            tuple.put(i, DatumFactory.createNullDatum());
          } else {
            String cell = cells[i].trim();

            if (cell.equals("")) {
              tuple.put(i, DatumFactory.createNullDatum());
            } else {
              switch (field.getDataType()) {
              case BOOLEAN:
                tuple.put(i, DatumFactory.createBool(cell));
                break;
              case BYTE:
                tuple.put(i,
                    DatumFactory.createByte(Base64.decodeBase64(cell)[0]));
                break;
              case CHAR:
                tuple.put(i, DatumFactory.createChar(cell.charAt(0)));
                break;
              case BYTES:
                tuple.put(i,
                    DatumFactory.createBytes(Base64.decodeBase64(cell)));
                break;
              case SHORT:
                tuple.put(i, DatumFactory.createShort(cell));
                break;
              case INT:
                tuple.put(i, DatumFactory.createInt(cell));
                break;
              case LONG:
                tuple.put(i, DatumFactory.createLong(cell));
                break;
              case FLOAT:
                tuple.put(i, DatumFactory.createFloat(cell));
                break;
              case DOUBLE:
                tuple.put(i, DatumFactory.createDouble(cell));
                break;
              case STRING:
                tuple.put(i, DatumFactory.createString(cell));
                break;
              case STRING2:
                tuple.put(i, DatumFactory.createString2(cell));
                break;
              case IPv4:
                tuple.put(i, DatumFactory.createIPv4(cell));
                break;
              case ARRAY:
                Datum data = GsonCreator.getInstance().fromJson(cell,
                    Datum.class);
                tuple.put(i, data);
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
      init(fragment);
    }

    @Override
    public void close() throws IOException {
      fis.close();
    }

    @Override
    public boolean isProjectable() {
      return false;
    }

    @Override
    public void setTarget(Column[] targets) {
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
      if (this.curTupleOffsetMap.containsKey(offset)) {
        this.currentIdx = this.curTupleOffsetMap.get(offset);
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
  }
}
