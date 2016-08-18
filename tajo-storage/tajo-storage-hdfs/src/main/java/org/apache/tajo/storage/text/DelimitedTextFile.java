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
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.compress.CodecPool;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.rcfile.NonSyncByteArrayOutputStream;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.ReflectionUtil;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
          new ConcurrentHashMap<>();

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
    serDeClassName = meta.getProperty(StorageConstants.TEXT_SERDE_CLASS, StorageConstants.DEFAULT_TEXT_SERDE_CLASS);

    try {
      Class<? extends TextLineSerDe> serdeClass;

      if (serdeClassCache.containsKey(serDeClassName)) {
        serdeClass = serdeClassCache.get(serDeClassName);
      } else {
        serdeClass = (Class<? extends TextLineSerDe>) Class.forName(serDeClassName);
        serdeClassCache.put(serDeClassName, serdeClass);
      }
      lineSerder = ReflectionUtil.newInstance(serdeClass);
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
      if (tableStatsEnabled) {
        this.stats = new TableStatistics(this.schema, columnStatsEnabled);
      }

      if(serializer != null) {
        serializer.release();
      }
      serializer = getLineSerde().createSerializer(schema, meta);
      serializer.init();

      bufferSize = conf.getInt(WRITE_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
      if (os == null) {
        os = new NonSyncByteArrayOutputStream(bufferSize);
      }
      os.reset();

      if (this.meta.containsProperty(StorageConstants.COMPRESSION_CODEC)) {
        String codecName = this.meta.getProperty(StorageConstants.COMPRESSION_CODEC);
        codecFactory = new CompressionCodecFactory(conf);
        codec = codecFactory.getCodecByClassName(codecName);
        compressor = CodecPool.getCompressor(codec);
        if (compressor != null) compressor.reset();  //builtin gzip is null

        String extension = codec.getDefaultExtension();
        compressedPath = path.suffix(extension);

        fos = fs.create(compressedPath, false);
        deflateFilter = codec.createOutputStream(fos, compressor);
        outputStream = new DataOutputStream(deflateFilter);

      } else {
        fos = fs.create(path, false);
        outputStream = new DataOutputStream(new BufferedOutputStream(fos));
      }

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
      if (tableStatsEnabled) {
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
      if(inited) {
        flushBuffer();
        outputStream.flush();
      }
    }

    @Override
    public void close() throws IOException {

      try {
        if(serializer != null) {
          serializer.release();
        }

        flush();

        // Statistical section
        if (tableStatsEnabled) {
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
      if (tableStatsEnabled) {
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
    private long recordCount = 0;

    private DelimitedLineReader reader;
    private TextLineDeserializer deserializer;

    private int errorPrintOutMaxNum = 5;
    /** Maximum number of permissible errors */
    private int errorTorrenceMaxNum;
    /** How many errors have occurred? */
    private int errorNum;

    private VTuple outTuple;

    public DelimitedTextFileScanner(Configuration conf, final Schema schema, final TableMeta meta,
                                    final Fragment fragment)
        throws IOException {
      super(conf, schema, meta, fragment);
      reader = new DelimitedLineReader(conf, this.fragment, conf.getInt(READ_BUFFER_SIZE, 128 * StorageUnit.KB));
      if (!reader.isCompressed()) {
        splittable = true;
      }

      startOffset = this.fragment.getStartKey();
      endOffset = this.fragment.getEndKey();

      errorTorrenceMaxNum =
          Integer.parseInt(meta.getProperty(TEXT_ERROR_TOLERANCE_MAXNUM, DEFAULT_TEXT_ERROR_TOLERANCE_MAXNUM));
    }


    @Override
    public void init() throws IOException {

      reader.init();

      if (targets == null) {
        targets = schema.toArray();
      }

      reset();

      super.init();
      if (LOG.isDebugEnabled()) {
        LOG.debug("DelimitedTextFileScanner open:" + fragment.getPath() + "," + startOffset + "," + endOffset);
      }
    }

    public TextLineSerDe getLineSerde() {
      return DelimitedTextFile.getLineSerde(meta);
    }

    @Override
    public float getProgress() {
      if(!inited) return super.getProgress();

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

      if (!reader.isReadable()) {
        return null;
      }

      // this loop will continue until one tuple is build or EOS (end of stream).
      do {
        long offset = reader.getUncompressedPosition();
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

        outTuple.setOffset(offset);

        try {
          deserializer.deserialize(buf, outTuple);
          // if a line is read normally, it exits this loop.
          break;

        } catch (TextLineParsingError tae) {

          errorNum++;

          // suppress too many log prints, which probably cause performance degradation
          if (errorNum < errorPrintOutMaxNum) {
            LOG.warn("Ignore Text Parse Error (" + errorNum + "): ", tae);
          }

          // Only when the maximum error torrence limit is set (i.e., errorTorrenceMaxNum >= 0),
          // it checks if the number of parsing error exceeds the max limit.
          // Otherwise, it will ignore all parsing errors.
          if (errorTorrenceMaxNum >= 0 && errorNum > errorTorrenceMaxNum) {
            throw new IOException(tae);
          }
        }
      } while (reader.isReadable()); // continue until EOS

      // recordCount means the number of actual read records. We increment the count here.
      recordCount++;

      return outTuple;
    }

    @Override
    public void reset() throws IOException {
      recordCount = 0;

      if (reader.getReadBytes() > 0) {
        reader.close();

        reader = new DelimitedLineReader(conf, fragment, conf.getInt(READ_BUFFER_SIZE, 128 * StorageUnit.KB));
        reader.init();
      }

      if(deserializer != null) {
        deserializer.release();
      }

      deserializer = getLineSerde().createDeserializer(schema, meta, targets);
      deserializer.init();

      outTuple = new VTuple(targets.length);

      // skip first line if it reads from middle of file
      if (startOffset > 0) {
        reader.readLine();
      } else { // skip header lines if it is defined

        // initialization for skipping header(max 20)
        int headerLineNum = Math.min(Integer.parseInt(
            meta.getProperty(StorageConstants.TEXT_SKIP_HEADER_LINE, "0")), 20);

        if (headerLineNum > 0) {
          LOG.info(String.format("Skip %d header lines", headerLineNum));
          for (int i = 0; i < headerLineNum; i++) {
            if (!reader.isReadable()) {
              return;
            }

            reader.readLine();
          }
        }
      }
    }

    @Override
    public void close() throws IOException {
      try {
        if (deserializer != null) {
          deserializer.release();
        }

        if (reader != null) {
          inputStats.setReadBytes(reader.getReadBytes());  //Actual Processed Bytes. (decompressed bytes + overhead)
          inputStats.setNumRows(recordCount);
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("DelimitedTextFileScanner processed record:" + recordCount);
        }
      } finally {
        IOUtils.cleanup(LOG, reader);
        outTuple = null;
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
    public void setFilter(EvalNode filter) {
      throw new TajoRuntimeException(new UnsupportedException());
    }

    @Override
    public boolean isSplittable() {
      return splittable;
    }

    @Override
    public TableStats getInputStats() {
      if (inputStats != null && reader != null) {
        inputStats.setReadBytes(reader.getReadBytes());  //Actual Processed Bytes. (decompressed bytes + overhead)
        inputStats.setNumRows(recordCount);
        inputStats.setNumBytes(fragment.getLength());
      }
      return inputStats;
    }

    @Override
    public long getNextOffset() throws IOException {
      return reader.getUncompressedPosition();
    }

    @Override
    public void seek(long offset) throws IOException {
        reader.seek(offset);
    }
  }
}
