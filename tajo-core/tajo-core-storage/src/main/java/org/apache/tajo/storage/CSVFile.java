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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.*;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.datum.*;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.exception.AlreadyExistsStorageException;
import org.apache.tajo.storage.json.StorageGsonHelper;
import org.apache.tajo.storage.compress.CodecPool;

import java.io.*;
import java.util.Arrays;

public class CSVFile {
  public static final String DELIMITER = "csvfile.delimiter";
  public static final String DELIMITER_DEFAULT = "|";
  public static final byte LF = '\n';
  private static final Log LOG = LogFactory.getLog(CSVFile.class);

  public static class CSVAppender extends FileAppender {
    private final TableMeta meta;
    private final Schema schema;
    private final FileSystem fs;
    private FSDataOutputStream fos;
    private DataOutputStream outputStream;
    private CompressionOutputStream deflateFilter;
    private String delimiter;
    private TableStatistics stats = null;
    private Compressor compressor;
    private CompressionCodecFactory codecFactory;
    private CompressionCodec codec;
    private Path compressedPath;

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

      String codecName = this.meta.getOption(TableMeta.COMPRESSION_CODEC);
      if(!StringUtils.isEmpty(codecName)){
        codecFactory = new CompressionCodecFactory(conf);
        codec = codecFactory.getCodecByClassName(codecName);
        compressor =  CodecPool.getCompressor(codec);
        if(compressor != null) compressor.reset();  //builtin gzip is null

        String extension = codec.getDefaultExtension();
        compressedPath = path.suffix(extension);

        if (fs.exists(compressedPath)) {
          throw new AlreadyExistsStorageException(compressedPath);
        }

        fos = fs.create(compressedPath);
        deflateFilter = codec.createOutputStream(fos, compressor);
        outputStream = new DataOutputStream(new BufferedOutputStream(deflateFilter));

      } else {
        if (fs.exists(path)) {
          throw new AlreadyExistsStorageException(path);
        }
        fos = fs.create(path);
        outputStream = fos;
      }

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
              CharDatum charDatum = tuple.getChar(i);
              sb.append(charDatum);
              byte[] pad = new byte[col.getDataType().getLength()-charDatum.size()];
              sb.append(new String(pad));
              break;
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
      outputStream.write(sb.toString().getBytes());
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
      outputStream.flush();
    }

    @Override
    public void close() throws IOException {
      // Statistical section
      if (enabledStats) {
        stats.setNumBytes(getOffset());
      }

      try {
        flush();

        if(deflateFilter != null) {
          deflateFilter.finish();
          deflateFilter.resetState();
          deflateFilter = null;
        }

        fos.close();
      } finally {
        if (compressor != null) {
          CodecPool.returnCompressor(compressor);
          compressor = null;
        }
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

    public boolean isCompress() {
      return compressor != null;
    }

    public String getExtension() {
      return codec != null ? codec.getDefaultExtension() : "";
    }
  }

  public static class CSVScanner extends FileScanner implements SeekableScanner {
    public CSVScanner(Configuration conf, final TableMeta meta,
                      final Fragment fragment) throws IOException {
      super(conf, meta, fragment);
      factory = new CompressionCodecFactory(conf);
      codec = factory.getCodec(fragment.getPath());
    }

    private final static int DEFAULT_BUFFER_SIZE = 256 * 1024;
    private int bufSize;
    private char delimiter;
    private FileSystem fs;
    private FSDataInputStream fis;
    private InputStream is; //decompressd stream
    private CompressionCodecFactory factory;
    private CompressionCodec codec;
    private Decompressor decompressor;
    private Seekable filePosition;
    private boolean splittable = true;
    private long startOffset, length;
    private byte[] buf = null;
    private String[] tuples = null;
    private long[] tupleOffsets = null;
    private int currentIdx = 0, validIdx = 0;
    private byte[] tail = null;
    private long pageStart = -1;
    private long prevTailLen = -1;
    private int[] targetColumnIndexes;
    private boolean eof = false;

    @Override
    public void init() throws IOException {

      // Buffer size, Delimiter
      this.bufSize = DEFAULT_BUFFER_SIZE;
      String delim  = fragment.getMeta().getOption(DELIMITER, DELIMITER_DEFAULT);
      this.delimiter = delim.charAt(0);

      // Fragment information
      fs = fragment.getPath().getFileSystem(conf);
      fis = fs.open(fragment.getPath());
      startOffset = fragment.getStartOffset();
      length = fragment.getLength();

      if(startOffset > 0) startOffset--; // prev line feed

      if (codec != null) {
        decompressor = CodecPool.getDecompressor(codec);
        if (codec instanceof SplittableCompressionCodec) {
          SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec).createInputStream(
              fis, decompressor, startOffset, startOffset + length,
              SplittableCompressionCodec.READ_MODE.BYBLOCK);

          startOffset = cIn.getAdjustedStart();
          length = cIn.getAdjustedEnd() - startOffset;
          filePosition = cIn;
          is = cIn;
        } else {
          is = new DataInputStream(codec.createInputStream(fis, decompressor));
          splittable = false;
        }
      } else {
        fis.seek(startOffset);
        filePosition = fis;
        is = fis;
      }

      tuples = new String[0];
      if (targets == null) {
        targets = schema.toArray();
      }

      targetColumnIndexes = new int[targets.length];
      for (int i = 0; i < targets.length; i++) {
        targetColumnIndexes[i] = schema.getColumnIdByName(targets[i].getColumnName());
      }
      super.init();

      if(LOG.isDebugEnabled()) {
        LOG.debug("CSVScanner open:" + fragment.getPath() + "," + startOffset + "," + length +
            "," + fs.getFileStatus(fragment.getPath()).getLen());
      }

      if (startOffset != 0) {
        int rbyte;
        while ((rbyte = is.read()) != LF) {
          if(rbyte == -1) break;
        }
      }

      if (fragmentable() < 1) {
        close();
        return;
      }
      page();
    }

    private long fragmentable() throws IOException {
      return startOffset + length - getFilePosition();
    }

    private long getFilePosition() throws IOException {
      long retVal;
      if (filePosition != null) {
        retVal = filePosition.getPos();
      } else {
        retVal = fis.getPos();
      }
      return retVal;
    }

    private void page() throws IOException {
      // Index initialization
      currentIdx = 0;

      // Buffer size set
      if (isSplittable() &&  fragmentable() < DEFAULT_BUFFER_SIZE) {
        bufSize = (int)fragmentable();
      }

      if (this.tail == null || this.tail.length == 0) {
        this.pageStart = getFilePosition();
        this.prevTailLen = 0;
      } else {
        this.pageStart = getFilePosition() - this.tail.length;
        this.prevTailLen = this.tail.length;
      }

      // Read
      int rbyte;
      buf = new byte[bufSize];
      rbyte = is.read(buf);

      if(rbyte < 0){
        eof = true; //EOF
        return;
      }

      if (prevTailLen == 0) {
        tail = new byte[0];
        tuples = StringUtils.split(new String(buf, 0, rbyte), (char)LF);
      } else {
        tuples = StringUtils.split(new String(tail) + new String(buf, 0, rbyte), (char)LF);
        tail = null;
      }

      // Check tail
      if ((char) buf[rbyte - 1] != LF) {
        if (isSplittable() && (fragmentable() < 1 || rbyte != bufSize)) {
          int cnt = 0;
          byte[] temp = new byte[DEFAULT_BUFFER_SIZE];
          // Read bytes
          while ((temp[cnt] = (byte)is.read()) != LF) {
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

     if(!isCompress()) makeTupleOffset();
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
          if (isSplittable() && fragmentable() < 1) {
            close();
            return null;
          } else {
            page();
          }

          if(eof){
            close();
            return null;
          }
        }


        long offset = -1;
        if(!isCompress()){
          offset = this.tupleOffsets[currentIdx];
        }

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
                  String trimmed = cell.trim();
                  tuple.put(tid, DatumFactory.createChar(trimmed));
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
        LOG.error("Tuple list length: " + (tuples != null ? tuples.length : 0), t);
        LOG.error("Tuple list current index: " + currentIdx, t);
      }
      return null;
    }

    private boolean isCompress() {
      return codec != null;
    }

    @Override
    public void reset() throws IOException {
      init();
    }

    @Override
    public void close() throws IOException {
      try {
        is.close();
      } finally {
        if (decompressor != null) {
          CodecPool.returnDecompressor(decompressor);
          decompressor = null;
        }
      }
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
      if(isCompress()) throw new UnsupportedException();

      int tupleIndex = Arrays.binarySearch(this.tupleOffsets, offset);
      if (tupleIndex > -1) {
        this.currentIdx = tupleIndex;
      } else if (isSplittable() && offset >= this.pageStart + this.bufSize
          + this.prevTailLen - this.tail.length || offset <= this.pageStart) {
        filePosition.seek(offset);
        tail = new byte[0];
        buf = new byte[DEFAULT_BUFFER_SIZE];
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
      if(isCompress()) throw new UnsupportedException();

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
      return splittable;
    }
  }
}
