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
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringEscapeUtils;
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
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.CharDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.datum.protobuf.ProtobufJsonFormat;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.compress.CodecPool;
import org.apache.tajo.storage.exception.AlreadyExistsStorageException;
import org.apache.tajo.util.Bytes;

import java.io.*;
import java.util.Arrays;

public class CSVFile {
  public static byte[] trueBytes = "true".getBytes();
  public static byte[] falseBytes = "false".getBytes();

  public static final String DELIMITER = "csvfile.delimiter";
  public static final String NULL = "csvfile.null";     //read only
  public static final String DELIMITER_DEFAULT = "|";
  public static final byte LF = '\n';
  public static int EOF = -1;

  private static final Log LOG = LogFactory.getLog(CSVFile.class);

  public static class CSVAppender extends FileAppender {
    private final TableMeta meta;
    private final Schema schema;
    private final FileSystem fs;
    private FSDataOutputStream fos;
    private DataOutputStream outputStream;
    private CompressionOutputStream deflateFilter;
    private char delimiter;
    private TableStatistics stats = null;
    private Compressor compressor;
    private CompressionCodecFactory codecFactory;
    private CompressionCodec codec;
    private Path compressedPath;
    private byte[] nullChars;
    private ProtobufJsonFormat protobufJsonFormat = ProtobufJsonFormat.getInstance();

    public CSVAppender(Configuration conf, final TableMeta meta,
                       final Path path) throws IOException {
      super(conf, meta, path);
      this.fs = path.getFileSystem(conf);
      this.meta = meta;
      this.schema = meta.getSchema();
      this.delimiter = StringEscapeUtils.unescapeJava(this.meta.getOption(DELIMITER, DELIMITER_DEFAULT)).charAt(0);

      String nullCharacters = StringEscapeUtils.unescapeJava(this.meta.getOption(NULL));
      if (StringUtils.isEmpty(nullCharacters)) {
        nullChars = NullDatum.get().asTextBytes();
      } else {
        nullChars = nullCharacters.getBytes();
      }
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
      Column col;
      Datum datum;

      int colNum = schema.getColumnNum();
      if (tuple instanceof LazyTuple) {
        LazyTuple  lTuple = (LazyTuple)tuple;
        for (int i = 0; i < colNum; i++) {
          TajoDataTypes.DataType dataType = schema.getColumn(i).getDataType();

          switch (dataType.getType()) {
            case TEXT: {
              datum = tuple.get(i);
              if (datum instanceof NullDatum) {
                outputStream.write(nullChars);
              } else {
                outputStream.write(datum.asTextBytes());
              }
              break;
            }
            case CHAR: {
              datum = tuple.get(i);
              if (datum instanceof NullDatum) {
                outputStream.write(nullChars);
              } else {
                byte[] pad = new byte[dataType.getLength() - datum.size()];
                outputStream.write(datum.asTextBytes());
                outputStream.write(pad);
              }
              break;
            }
            case BOOLEAN: {
              datum = tuple.get(i);
              if (datum instanceof NullDatum) {
                //null datum is zero length byte array
              } else {
                outputStream.write(datum.asBool() ? trueBytes : falseBytes);   //Compatibility with Apache Hive
              }
              break;
            }
            case NULL:
              break;
            case PROTOBUF:
              datum = tuple.get(i);
              ProtobufDatum protobufDatum = (ProtobufDatum) datum;
              protobufJsonFormat.print(protobufDatum.get(), outputStream);
              break;
            default:
              outputStream.write(lTuple.getTextBytes(i)); //better usage for insertion to table of lazy tuple
              break;
          }

          if(colNum - 1 > i){
            outputStream.write((byte) delimiter);
          }

          if (enabledStats) {
            datum = tuple.get(i);
            stats.analyzeField(i, datum);
          }
        }
      } else {
        for (int i = 0; i < schema.getColumnNum(); i++) {
          datum = tuple.get(i);
          if (enabledStats) {
            stats.analyzeField(i, datum);
          }
          if (datum instanceof NullDatum) {
            outputStream.write(nullChars);
          } else {
            col = schema.getColumn(i);
            switch (col.getDataType().getType()) {
              case BOOLEAN:
                outputStream.write(tuple.getBoolean(i).asBool() ? trueBytes : falseBytes);   //Compatibility with Apache Hive
                break;
              case BIT:
                outputStream.write(tuple.getByte(i).asTextBytes());
                break;
              case BLOB:
                outputStream.write(Base64.encodeBase64(tuple.getBytes(i).asByteArray(), false));
                break;
              case CHAR:
                CharDatum charDatum = tuple.getChar(i);
                byte[] pad = new byte[col.getDataType().getLength() - datum.size()];
                outputStream.write(charDatum.asTextBytes());
                outputStream.write(pad);
                break;
              case TEXT:
                outputStream.write(tuple.getText(i).asTextBytes());
                break;
              case INT2:
                outputStream.write(tuple.getShort(i).asTextBytes());
                break;
              case INT4:
                outputStream.write(tuple.getInt(i).asTextBytes());
                break;
              case INT8:
                outputStream.write(tuple.getLong(i).asTextBytes());
                break;
              case FLOAT4:
                outputStream.write(tuple.getFloat(i).asTextBytes());
                break;
              case FLOAT8:
                outputStream.write(tuple.getDouble(i).asTextBytes());
                break;
              case INET4:
                outputStream.write(tuple.getIPv4(i).asTextBytes());
                break;
              case INET6:
                outputStream.write(tuple.getIPv6(i).toString().getBytes());
                break;
              case PROTOBUF:
                ProtobufDatum protobuf = (ProtobufDatum) datum;
                ProtobufJsonFormat.getInstance().print(protobuf.get(), outputStream);
                break;
              default:
                throw new UnsupportedOperationException("Cannot write such field: "
                    + tuple.get(i).type());
            }
          }
          if(colNum - 1 > i){
            outputStream.write((byte) delimiter);
          }
        }
      }
      // Statistical section
      outputStream.write('\n');
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
      if (isCompress() && !(codec instanceof SplittableCompressionCodec)) {
          splittable = false;
      }

      // Buffer size, Delimiter
      this.bufSize = DEFAULT_BUFFER_SIZE;
      String delim  = fragment.getMeta().getOption(DELIMITER, DELIMITER_DEFAULT);
      this.delimiter = StringEscapeUtils.unescapeJava(delim).charAt(0);

      String nullCharacters = StringEscapeUtils.unescapeJava(fragment.getMeta().getOption(NULL));
      if (StringUtils.isEmpty(nullCharacters)) {
        nullChars = NullDatum.get().asTextBytes();
      } else {
        nullChars = nullCharacters.getBytes();
      }
    }

    private final static int DEFAULT_BUFFER_SIZE = 128 * 1024;
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
    private byte[][] tuples = null;
    private long[] tupleOffsets = null;
    private int currentIdx = 0, validIdx = 0;
    private byte[] tail = null;
    private long pageStart = -1;
    private long prevTailLen = -1;
    private int[] targetColumnIndexes;
    private boolean eof = false;
    private final byte[] nullChars;

    @Override
    public void init() throws IOException {

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
        }
      } else {
        fis.seek(startOffset);
        filePosition = fis;
        is = fis;
      }

      tuples = new byte[0][];
      if (targets == null) {
        targets = schema.toArray();
      }

      targetColumnIndexes = new int[targets.length];
      for (int i = 0; i < targets.length; i++) {
        targetColumnIndexes[i] = schema.getColumnIdByName(targets[i].getColumnName());
      }

      super.init();
      Arrays.sort(targetColumnIndexes);
      if (LOG.isDebugEnabled()) {
        LOG.debug("CSVScanner open:" + fragment.getPath() + "," + startOffset + "," + length +
            "," + fs.getFileStatus(fragment.getPath()).getLen());
      }

      if (startOffset != 0) {
        int rbyte;
        while ((rbyte = is.read()) != LF) {
          if(rbyte == EOF) break;
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

      if (prevTailLen == 0) {
        if(rbyte == EOF){
          eof = true; //EOF
          return;
        }

        tail = new byte[0];
        tuples = Bytes.splitPreserveAllTokens(buf, rbyte, (char) LF);
      } else {
        byte[] lastRow = ArrayUtils.addAll(tail, buf);
        tuples = Bytes.splitPreserveAllTokens(lastRow, rbyte + tail.length, (char) LF);
        tail = null;
      }

      // Check tail
      if ((char) buf[rbyte - 1] != LF) {
        // splittable bzip2 compression returned 1 byte when sync maker found
        if (isSplittable() && (fragmentable() < 1 || rbyte != bufSize)) {
          int lineFeedPos = 0;
          byte[] temp = new byte[DEFAULT_BUFFER_SIZE];

          // find line feed
          while ((temp[lineFeedPos] = (byte)is.read()) != (byte)LF) {
            lineFeedPos++;
          }

          tuples[tuples.length - 1] = ArrayUtils.addAll(tuples[tuples.length - 1],
              ArrayUtils.subarray(temp, 0, lineFeedPos));
          validIdx = tuples.length;

        } else {
          tail = tuples[tuples.length - 1];
          validIdx = tuples.length - 1;
        }
      } else {
        tail = new byte[0];
        validIdx = tuples.length - 1;   //remove last empty row ( .... \n .... \n  length is 3)
      }

     if(!isCompress()) makeTupleOffset();
    }

    private void makeTupleOffset() {
      long curTupleOffset = 0;
      this.tupleOffsets = new long[this.validIdx];
      for (int i = 0; i < this.validIdx; i++) {
        this.tupleOffsets[i] = curTupleOffset + this.pageStart;
        curTupleOffset += this.tuples[i].length + 1;//tuple byte +  1byte line feed
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

        byte[][] cells = Bytes.splitPreserveAllTokens(tuples[currentIdx++], delimiter, targetColumnIndexes);
        return new LazyTuple(schema, cells, offset, nullChars);
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
          decompressor.reset();
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
