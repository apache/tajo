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

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.compress.CodecPool;
import org.apache.tajo.storage.exception.AlreadyExistsStorageException;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.rcfile.NonSyncByteArrayOutputStream;
import org.apache.tajo.util.Bytes;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

public class CSVFile {

  public static final byte LF = '\n';
  public static int EOF = -1;

  private static final Log LOG = LogFactory.getLog(CSVFile.class);

  public static class CSVAppender extends FileAppender {
    private final TableMeta meta;
    private final Schema schema;
    private final int columnNum;
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
    private int BUFFER_SIZE = 128 * 1024;
    private int bufferedBytes = 0;
    private long pos = 0;
    private boolean isShuffle;

    private NonSyncByteArrayOutputStream os = new NonSyncByteArrayOutputStream(BUFFER_SIZE);
    private SerializerDeserializer serde;

    public CSVAppender(Configuration conf, final Schema schema, final TableMeta meta, final Path path) throws IOException {
      super(conf, schema, meta, path);
      this.fs = path.getFileSystem(conf);
      this.meta = meta;
      this.schema = schema;
      this.delimiter = StringEscapeUtils.unescapeJava(this.meta.getOption(StorageConstants.CSVFILE_DELIMITER,
          StorageConstants.DEFAULT_FIELD_DELIMITER)).charAt(0);
      this.columnNum = schema.size();
      String nullCharacters = StringEscapeUtils.unescapeJava(this.meta.getOption(StorageConstants.CSVFILE_NULL));
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

      //determine the intermediate file type
      String store = conf.get(TajoConf.ConfVars.SHUFFLE_FILE_FORMAT.varname,
          TajoConf.ConfVars.SHUFFLE_FILE_FORMAT.defaultVal);
      if (enabledStats && CatalogProtos.StoreType.CSV == CatalogProtos.StoreType.valueOf(store.toUpperCase())) {
        isShuffle = true;
      } else {
        isShuffle = false;
      }

      String codecName = this.meta.getOption(StorageConstants.COMPRESSION_CODEC);
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
        outputStream = new DataOutputStream(deflateFilter);

      } else {
        if (fs.exists(path)) {
          throw new AlreadyExistsStorageException(path);
        }
        fos = fs.create(path);
        outputStream = new DataOutputStream(new BufferedOutputStream(fos));
      }

      if (enabledStats) {
        this.stats = new TableStatistics(this.schema);
      }

      try {
        String serdeClass = this.meta.getOption(StorageConstants.CSVFILE_SERDE,
            TextSerializerDeserializer.class.getName());
        serde = (SerializerDeserializer) Class.forName(serdeClass).newInstance();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        throw new IOException(e);
      }

      os.reset();
      pos = fos.getPos();
      bufferedBytes = 0;
      super.init();
    }


    @Override
    public void addTuple(Tuple tuple) throws IOException {
      Datum datum;
      int rowBytes = 0;

      for (int i = 0; i < columnNum; i++) {
        datum = tuple.get(i);
        rowBytes += serde.serialize(schema.getColumn(i), datum, os, nullChars);

        if(columnNum - 1 > i){
          os.write((byte) delimiter);
          rowBytes += 1;
        }
        if (isShuffle) {
          // it is to calculate min/max values, and it is only used for the intermediate file.
          stats.analyzeField(i, datum);
        }
      }
      os.write(LF);
      rowBytes += 1;

      pos += rowBytes;
      bufferedBytes += rowBytes;
      if(bufferedBytes > BUFFER_SIZE){
        flushBuffer();
      }
      // Statistical section
      if (enabledStats) {
        stats.incrementRow();
      }
    }

    private void flushBuffer() throws IOException {
      if(os.getLength() > 0) {
        os.writeTo(outputStream);
        os.reset();
        bufferedBytes = 0;
      }
    }
    @Override
    public long getOffset() throws IOException {
      return pos;
    }

    @Override
    public void flush() throws IOException {
      flushBuffer();
      outputStream.flush();
    }

    @Override
    public void close() throws IOException {

      try {
        flush();

        // Statistical section
        if (enabledStats) {
          stats.setNumBytes(getOffset());
        }

        if(deflateFilter != null) {
          deflateFilter.finish();
          deflateFilter.resetState();
          deflateFilter = null;
        }

        os.close();
      } finally {
        IOUtils.cleanup(LOG, fos);
        if (compressor != null) {
          CodecPool.returnCompressor(compressor);
          compressor = null;
        }
      }
    }

    @Override
    public TableStats getStats() {
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
    public CSVScanner(Configuration conf, final Schema schema, final TableMeta meta, final FileFragment fragment)
        throws IOException {
      super(conf, schema, meta, fragment);
      factory = new CompressionCodecFactory(conf);
      codec = factory.getCodec(fragment.getPath());
      if (codec == null || codec instanceof SplittableCompressionCodec) {
        splittable = true;
      }

      //Delimiter
      String delim  = meta.getOption(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
      this.delimiter = StringEscapeUtils.unescapeJava(delim).charAt(0);

      String nullCharacters = StringEscapeUtils.unescapeJava(meta.getOption(StorageConstants.CSVFILE_NULL));
      if (StringUtils.isEmpty(nullCharacters)) {
        nullChars = NullDatum.get().asTextBytes();
      } else {
        nullChars = nullCharacters.getBytes();
      }
    }

    private final static int DEFAULT_PAGE_SIZE = 256 * 1024;
    private char delimiter;
    private FileSystem fs;
    private FSDataInputStream fis;
    private InputStream is; //decompressd stream
    private CompressionCodecFactory factory;
    private CompressionCodec codec;
    private Decompressor decompressor;
    private Seekable filePosition;
    private boolean splittable = false;
    private long startOffset, end, pos;
    private int currentIdx = 0, validIdx = 0, recordCount = 0;
    private int[] targetColumnIndexes;
    private boolean eof = false;
    private final byte[] nullChars;
    private SplitLineReader reader;
    private ArrayList<Long> fileOffsets;
    private ArrayList<Integer> rowLengthList;
    private ArrayList<Integer> startOffsets;
    private NonSyncByteArrayOutputStream buffer;
    private SerializerDeserializer serde;

    @Override
    public void init() throws IOException {
      fileOffsets = new ArrayList<Long>();
      rowLengthList = new ArrayList<Integer>();
      startOffsets = new ArrayList<Integer>();
      buffer = new NonSyncByteArrayOutputStream(DEFAULT_PAGE_SIZE);

      // FileFragment information
      if(fs == null) {
        fs = FileScanner.getFileSystem((TajoConf)conf, fragment.getPath());
      }
      if(fis == null) fis = fs.open(fragment.getPath());

      recordCount = 0;
      pos = startOffset = fragment.getStartKey();
      end = startOffset + fragment.getEndKey();

      if (codec != null) {
        decompressor = CodecPool.getDecompressor(codec);
        if (codec instanceof SplittableCompressionCodec) {
          SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec).createInputStream(
              fis, decompressor, startOffset, end,
              SplittableCompressionCodec.READ_MODE.BYBLOCK);

          reader = new CompressedSplitLineReader(cIn, conf, null);
          startOffset = cIn.getAdjustedStart();
          end = cIn.getAdjustedEnd();
          filePosition = cIn;
          is = cIn;
        } else {
          is = new DataInputStream(codec.createInputStream(fis, decompressor));
          reader = new SplitLineReader(is, null);
          filePosition = fis;
        }
      } else {
        fis.seek(startOffset);
        filePosition = fis;
        is = fis;
        reader = new SplitLineReader(is, null);
      }

      if (targets == null) {
        targets = schema.toArray();
      }

      targetColumnIndexes = new int[targets.length];
      for (int i = 0; i < targets.length; i++) {
        targetColumnIndexes[i] = schema.getColumnId(targets[i].getQualifiedName());
      }

      try {
        String serdeClass = this.meta.getOption(StorageConstants.CSVFILE_SERDE,
            TextSerializerDeserializer.class.getName());
        serde = (SerializerDeserializer) Class.forName(serdeClass).newInstance();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        throw new IOException(e);
      }

      super.init();
      Arrays.sort(targetColumnIndexes);
      if (LOG.isDebugEnabled()) {
        LOG.debug("CSVScanner open:" + fragment.getPath() + "," + startOffset + "," + end +
            "," + fs.getFileStatus(fragment.getPath()).getLen());
      }

      if (startOffset != 0) {
        pos += reader.readLine(new Text(), 0, maxBytesToConsume(pos));
      }
      eof = false;
      page();
    }

    private int maxBytesToConsume(long pos) {
      return isCompress() ? Integer.MAX_VALUE : (int) Math.min(Integer.MAX_VALUE, end - pos);
    }

    private long fragmentable() throws IOException {
      return end - getFilePosition();
    }

    private long getFilePosition() throws IOException {
      long retVal;
      if (isCompress()) {
        retVal = filePosition.getPos();
      } else {
        retVal = pos;
      }
      return retVal;
    }

    private void page() throws IOException {
//      // Index initialization
      currentIdx = 0;
      validIdx = 0;
      int currentBufferPos = 0;
      int bufferedSize = 0;

      buffer.reset();
      startOffsets.clear();
      rowLengthList.clear();
      fileOffsets.clear();

      if(eof) {
        return;
      }

      while (DEFAULT_PAGE_SIZE >= bufferedSize){

        int ret = reader.readDefaultLine(buffer, rowLengthList, Integer.MAX_VALUE, Integer.MAX_VALUE);

        if(ret == 0){
          break;
        } else {
          fileOffsets.add(pos);
          pos += ret;
          startOffsets.add(currentBufferPos);
          currentBufferPos += rowLengthList.get(rowLengthList.size() - 1);
          bufferedSize += ret;
          validIdx++;
          recordCount++;
        }

        if(getFilePosition() > end && !reader.needAdditionalRecordAfterSplit()){
          eof = true;
          break;
        }
      }
      if (tableStats != null) {
        tableStats.setReadBytes(pos - startOffset);
        tableStats.setNumRows(recordCount);
      }
    }

    @Override
    public float getProgress() {
      try {
        if(eof) {
          return 1.0f;
        }
        long filePos = getFilePosition();
        if (startOffset == filePos) {
          return 0.0f;
        } else {
          long readBytes = filePos - startOffset;
          long remainingBytes = Math.max(end - filePos, 0);
          return Math.min(1.0f, (float)(readBytes) / (float)(readBytes + remainingBytes));
        }
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        return 0.0f;
      }
    }

    @Override
    public Tuple next() throws IOException {
      try {
        if (currentIdx == validIdx) {
          if (eof) {
            return null;
          } else {
            page();

            if(currentIdx == validIdx){
              return null;
            }
          }
        }

        long offset = -1;
        if(!isCompress()){
          offset = fileOffsets.get(currentIdx);
        }

        byte[][] cells = Bytes.splitPreserveAllTokens(buffer.getData(), startOffsets.get(currentIdx),
            rowLengthList.get(currentIdx),  delimiter, targetColumnIndexes);
        currentIdx++;
        return new LazyTuple(schema, cells, offset, nullChars, serde);
      } catch (Throwable t) {
        LOG.error("Tuple list length: " + (fileOffsets != null ? fileOffsets.size() : 0), t);
        LOG.error("Tuple list current index: " + currentIdx, t);
        throw new IOException(t);
      }
    }

    private boolean isCompress() {
      return codec != null;
    }

    @Override
    public void reset() throws IOException {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }

      init();
    }

    @Override
    public void close() throws IOException {
      try {
        if (tableStats != null) {
          tableStats.setReadBytes(pos - startOffset);  //Actual Processed Bytes. (decompressed bytes + overhead)
          tableStats.setNumRows(recordCount);
        }

        IOUtils.cleanup(LOG, reader, is, fis);
        fs = null;
        is = null;
        fis = null;
        if (LOG.isDebugEnabled()) {
          LOG.debug("CSVScanner processed record:" + recordCount);
        }
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

      int tupleIndex = Arrays.binarySearch(fileOffsets.toArray(), offset);

      if (tupleIndex > -1) {
        this.currentIdx = tupleIndex;
      } else if (isSplittable() && end >= offset || startOffset <= offset) {
        eof = false;
        fis.seek(offset);
        pos = offset;
        reader.reset();
        this.currentIdx = 0;
        this.validIdx = 0;
        // pageBuffer();
      } else {
        throw new IOException("invalid offset " +
            " < start : " +  startOffset + " , " +
            "  end : " + end + " , " +
            "  filePos : " + filePosition.getPos() + " , " +
            "  input offset : " + offset + " >");
      }
    }

    @Override
    public long getNextOffset() throws IOException {
      if(isCompress()) throw new UnsupportedException();

      if (this.currentIdx == this.validIdx) {
        if (fragmentable() <= 0) {
          return -1;
        } else {
          page();
          if(currentIdx == validIdx) return -1;
        }
      }
      return fileOffsets.get(currentIdx);
    }

    @Override
    public boolean isSplittable(){
      return splittable;
    }
  }
}
