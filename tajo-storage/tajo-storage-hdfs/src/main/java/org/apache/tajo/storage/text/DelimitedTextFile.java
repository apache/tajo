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

package org.apache.tajo.storage.text;

import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.compress.CodecPool;
import org.apache.tajo.storage.exception.AlreadyExistsStorageException;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.rcfile.NonSyncByteArrayOutputStream;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.ReflectionUtil;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.tajo.storage.StorageConstants.DEFAULT_TEXT_ERROR_TOLERANCE_MAXNUM;
import static org.apache.tajo.storage.StorageConstants.TEXT_ERROR_TOLERANCE_MAXNUM;

public class DelimitedTextFile {

  public static final byte LF = '\n';
  public static final String READ_BUFFER_SIZE = "tajo.storage.text.io.read-buffer.bytes";
  public static final String WRITE_BUFFER_SIZE = "tajo.storage.text.io.write-buffer.bytes";
  public static final int DEFAULT_BUFFER_SIZE = 128 * StorageUnit.KB;

  private static final Log LOG = LogFactory.getLog(DelimitedTextFile.class);

  /** it caches line serde classes. */
  private static final Map<String, Class<? extends TextLineSerDe>> serdeClassCache =
      new ConcurrentHashMap<String, Class<? extends TextLineSerDe>>();

  /**
   * By default, DelimitedTextFileScanner uses CSVLineSerder. If a table property 'text.serde.class' is given,
   * it will use the specified serder class.
   *
   * @return TextLineSerder
   */
  public static TextLineSerDe getLineSerde(TableMeta meta) {
    TextLineSerDe lineSerder;

    String serDeClassName;

    // if there is no given serde class, it will use CSV line serder.
    serDeClassName = meta.getOption(StorageConstants.TEXT_SERDE_CLASS, StorageConstants.DEFAULT_TEXT_SERDE_CLASS);

    try {
      Class<? extends TextLineSerDe> serdeClass;

      if (serdeClassCache.containsKey(serDeClassName)) {
        serdeClass = serdeClassCache.get(serDeClassName);
      } else {
        serdeClass = (Class<? extends TextLineSerDe>) Class.forName(serDeClassName);
        serdeClassCache.put(serDeClassName, serdeClass);
      }
      lineSerder = (TextLineSerDe) ReflectionUtil.newInstance(serdeClass);
    } catch (Throwable e) {
      throw new RuntimeException("TextLineSerde class cannot be initialized.", e);
    }

    return lineSerder;
  }

  public static class DelimitedTextFileAppender extends FileAppender {
    private final TableMeta meta;
    private final Schema schema;
    private final FileSystem fs;
    private FSDataOutputStream fos;
    private DataOutputStream outputStream;
    private CompressionOutputStream deflateFilter;
    private TableStatistics stats = null;
    private Compressor compressor;
    private CompressionCodecFactory codecFactory;
    private CompressionCodec codec;
    private Path compressedPath;
    private int bufferSize;
    private int bufferedBytes = 0;
    private long pos = 0;

    private NonSyncByteArrayOutputStream os;
    private TextLineSerializer serializer;

    public DelimitedTextFileAppender(Configuration conf, TaskAttemptId taskAttemptId,
                                     final Schema schema, final TableMeta meta, final Path path)
        throws IOException {
      super(conf, taskAttemptId, schema, meta, path);
      this.fs = path.getFileSystem(conf);
      this.meta = meta;
      this.schema = schema;
    }

    public TextLineSerDe getLineSerde() {
      return DelimitedTextFile.getLineSerde(meta);
    }

    @Override
    public void init() throws IOException {
      if (!fs.exists(path.getParent())) {
        throw new FileNotFoundException(path.toString());
      }

      if (this.meta.containsOption(StorageConstants.COMPRESSION_CODEC)) {
        String codecName = this.meta.getOption(StorageConstants.COMPRESSION_CODEC);
        codecFactory = new CompressionCodecFactory(conf);
        codec = codecFactory.getCodecByClassName(codecName);
        compressor = CodecPool.getCompressor(codec);
        if (compressor != null) compressor.reset();  //builtin gzip is null

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

      serializer = getLineSerde().createSerializer(schema, meta);
      serializer.init();

      bufferSize = conf.getInt(WRITE_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
      if (os == null) {
        os = new NonSyncByteArrayOutputStream(bufferSize);
      }

      os.reset();
      pos = fos.getPos();
      bufferedBytes = 0;
      super.init();
    }

    @Override
    public void addTuple(Tuple tuple) throws IOException {
      // write
      int rowBytes = serializer.serialize(os, tuple);

      // new line
      os.write(LF);
      rowBytes += 1;

      // update positions
      pos += rowBytes;
      bufferedBytes += rowBytes;

      // refill buffer if necessary
      if (bufferedBytes > bufferSize) {
        flushBuffer();
      }
      // Statistical section
      if (enabledStats) {
        stats.incrementRow();
      }
    }

    private void flushBuffer() throws IOException {
      if (os.getLength() > 0) {
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
        serializer.release();

        if(outputStream != null){
          flush();
        }

        // Statistical section
        if (enabledStats) {
          stats.setNumBytes(getOffset());
        }

        if (deflateFilter != null) {
          deflateFilter.finish();
          deflateFilter.resetState();
          deflateFilter = null;
        }
      } finally {
        IOUtils.cleanup(LOG, os, fos);
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

  public static class DelimitedTextFileScanner extends FileScanner implements SeekableScanner {
    private boolean splittable = false;
    private final long startOffset;

    private final long endOffset;
    /** The number of actual read records */
    private int recordCount = 0;
    private int[] targetColumnIndexes;

    private DelimitedLineReader reader;
    private TextLineDeserializer deserializer;

    private int errorPrintOutMaxNum = 5;
    /** Maximum number of permissible errors */
    private int errorTorrenceMaxNum;
    /** How many errors have occurred? */
    private int errorNum;

    public DelimitedTextFileScanner(Configuration conf, final Schema schema, final TableMeta meta,
                                    final Fragment fragment)
        throws IOException {
      super(conf, schema, meta, fragment);
      reader = new DelimitedLineReader(conf, this.fragment, conf.getInt(READ_BUFFER_SIZE, 128 * StorageUnit.KB));
      if (!reader.isCompressed()) {
        splittable = true;
      }

      startOffset = this.fragment.getStartKey();
      endOffset = startOffset + fragment.getLength();

      errorTorrenceMaxNum =
          Integer.parseInt(meta.getOption(TEXT_ERROR_TOLERANCE_MAXNUM, DEFAULT_TEXT_ERROR_TOLERANCE_MAXNUM));
    }


    @Override
    public void init() throws IOException {
      if (reader != null) {
        reader.close();
      }

      if(deserializer != null) {
        deserializer.release();
      }

      reader = new DelimitedLineReader(conf, fragment, conf.getInt(READ_BUFFER_SIZE, 128 * StorageUnit.KB));
      reader.init();
      recordCount = 0;

      if (targets == null) {
        targets = schema.toArray();
      }

      targetColumnIndexes = new int[targets.length];
      for (int i = 0; i < targets.length; i++) {
        targetColumnIndexes[i] = schema.getColumnId(targets[i].getQualifiedName());
      }

      super.init();
      Arrays.sort(targetColumnIndexes);
      if (LOG.isDebugEnabled()) {
        LOG.debug("DelimitedTextFileScanner open:" + fragment.getPath() + "," + startOffset + "," + endOffset);
      }

      if (startOffset > 0) {
        reader.readLine();  // skip first line;
      }

      deserializer = getLineSerde().createDeserializer(schema, meta, targetColumnIndexes);
      deserializer.init();
    }

    public TextLineSerDe getLineSerde() {
      return DelimitedTextFile.getLineSerde(meta);
    }

    @Override
    public float getProgress() {
      try {
        if (!reader.isReadable()) {
          return 1.0f;
        }
        long filePos = reader.getCompressedPosition();
        if (startOffset == filePos) {
          return 0.0f;
        } else {
          long readBytes = filePos - startOffset;
          long remainingBytes = Math.max(endOffset - filePos, 0);
          return Math.min(1.0f, (float) (readBytes) / (float) (readBytes + remainingBytes));
        }
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        return 0.0f;
      }
    }

    @Override
    public Tuple next() throws IOException {
      VTuple tuple;

      if (!reader.isReadable()) {
        return null;
      }

      try {

        // this loop will continue until one tuple is build or EOS (end of stream).
        do {
          long offset = reader.getUnCompressedPosition();
          ByteBuf buf = reader.readLine();

          // if no more line, then return EOT (end of tuple)
          if (buf == null) {
            return null;
          }

          // If there is no required column, we just read each line
          // and then return an empty tuple without parsing line.
          if (targets.length == 0) {
            recordCount++;
            return EmptyTuple.get();
          }

          tuple = new VTuple(schema.size());
          tuple.setOffset(offset);

          try {
            deserializer.deserialize(buf, tuple);
            // if a line is read normally, it exists this loop.
            break;

          } catch (TextLineParsingError tae) {

            errorNum++;

            // suppress too many log prints, which probably cause performance degradation
            if (errorNum < errorPrintOutMaxNum) {
              LOG.warn("Ignore JSON Parse Error (" + errorNum + "): ", tae);
            }

            // Only when the maximum error torrence limit is set (i.e., errorTorrenceMaxNum >= 0),
            // it checks if the number of parsing error exceeds the max limit.
            // Otherwise, it will ignore all parsing errors.
            if (errorTorrenceMaxNum >= 0 && errorNum > errorTorrenceMaxNum) {
              throw tae;
            }
            continue;
          }

        } while (reader.isReadable()); // continue until EOS

        // recordCount means the number of actual read records. We increment the count here.
        recordCount++;

        return tuple;

      } catch (Throwable t) {
        LOG.error(t);
        throw new IOException(t);
      }
    }

    @Override
    public void reset() throws IOException {
      init();
    }

    @Override
    public void close() throws IOException {
      try {
        if (deserializer != null) {
          deserializer.release();
        }

        if (tableStats != null && reader != null) {
          tableStats.setReadBytes(reader.getReadBytes());  //Actual Processed Bytes. (decompressed bytes + overhead)
          tableStats.setNumRows(recordCount);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("DelimitedTextFileScanner processed record:" + recordCount);
        }
      } finally {
        IOUtils.cleanup(LOG, reader);
        reader = null;
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
    public boolean isSplittable() {
      return splittable;
    }

    @Override
    public TableStats getInputStats() {
      if (tableStats != null && reader != null) {
        tableStats.setReadBytes(reader.getReadBytes());  //Actual Processed Bytes. (decompressed bytes + overhead)
        tableStats.setNumRows(recordCount);
        tableStats.setNumBytes(fragment.getLength());
      }
      return tableStats;
    }

    @Override
    public long getNextOffset() throws IOException {
      return reader.getUnCompressedPosition();
    }

    @Override
    public void seek(long offset) throws IOException {
        reader.seek(offset);
    }
  }
}
