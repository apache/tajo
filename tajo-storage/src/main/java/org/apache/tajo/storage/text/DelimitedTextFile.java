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
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.compress.CodecPool;
import org.apache.tajo.storage.exception.AlreadyExistsStorageException;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.rcfile.NonSyncByteArrayOutputStream;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DelimitedTextFile {

  public static final byte LF = '\n';
  public static int EOF = -1;

  private static final Log LOG = LogFactory.getLog(DelimitedTextFile.class);

  public static class DelimitedTextFileAppender extends FileAppender {
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

    private NonSyncByteArrayOutputStream os;
    private FieldSerializerDeserializer serde;

    public DelimitedTextFileAppender(Configuration conf, final Schema schema, final TableMeta meta, final Path path)
        throws IOException {
      super(conf, schema, meta, path);
      this.fs = path.getFileSystem(conf);
      this.meta = meta;
      this.schema = schema;
      this.delimiter = StringEscapeUtils.unescapeJava(this.meta.getOption(StorageConstants.TEXT_DELIMITER,
          StorageConstants.DEFAULT_FIELD_DELIMITER)).charAt(0);
      this.columnNum = schema.size();

      String nullCharacters = StringEscapeUtils.unescapeJava(this.meta.getOption(StorageConstants.TEXT_NULL,
          NullDatum.DEFAULT_TEXT));
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

      serde = new TextFieldSerializerDeserializer();

      if (os == null) {
        os = new NonSyncByteArrayOutputStream(BUFFER_SIZE);
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
        rowBytes += serde.serialize(os, datum, schema.getColumn(i), i, nullChars);

        if (columnNum - 1 > i) {
          os.write((byte) delimiter);
          rowBytes += 1;
        }
      }
      os.write(LF);
      rowBytes += 1;

      pos += rowBytes;
      bufferedBytes += rowBytes;
      if (bufferedBytes > BUFFER_SIZE) {
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

  public static class DelimitedTextFileScanner extends FileScanner {
    private static final Class<?> SERDE_CONSTRUCTOR_PARAM[] = {Schema.class, TableMeta.class, int[].class};
    /** it caches line serde constructors. */
    private static final Map<String, Constructor<? extends TextLineSerde>> serdeConstructorCache =
        new ConcurrentHashMap<String, Constructor<? extends TextLineSerde>>();

    private boolean splittable = false;
    private final long startOffset;

    private final long endOffset;
    private int recordCount = 0;
    private int[] targetColumnIndexes;

    private DelimitedLineReader reader;
    private TextLineSerde lineSerder;

    public DelimitedTextFileScanner(Configuration conf, final Schema schema, final TableMeta meta,
                                    final FileFragment fragment)
        throws IOException {
      super(conf, schema, meta, fragment);
      reader = new DelimitedLineReader(conf, fragment);
      if (!reader.isCompressed()) {
        splittable = true;
      }

      startOffset = fragment.getStartKey();
      endOffset = startOffset + fragment.getEndKey();
    }

    @Override
    public void init() throws IOException {
      if (reader != null) {
        reader.close();
      }
      reader = new DelimitedLineReader(conf, fragment);
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

      lineSerder = getLineSerder();
      lineSerder.init();
    }

    /**
     * By default, DelimitedTextFileScanner uses CSVLineSerder. If a table property 'text.serde.class' is given,
     * it will use the specified serder class.
     *
     * @return TextLineSerder
     */
    public TextLineSerde getLineSerder() {
      TextLineSerde lineSerder;

      String className;

      // if there is no given serde class, it will use CSV line serder.
      if (meta.containsOption(StorageConstants.TEXT_SERDE_CLASS)) {
        className = meta.getOption(StorageConstants.TEXT_SERDE_CLASS);
      } else {
        className = StorageConstants.DEFAULT_TEXT_SERDE_CLASS;
      }

      try {
        Constructor<? extends TextLineSerde> constructor;
        if (serdeConstructorCache.containsKey(className)) {
          constructor = serdeConstructorCache.get(className);
        } else {
          Class<? extends TextLineSerde> serDerClass = (Class<? extends TextLineSerde>) Class.forName(CSVLineSerde.class.getName());
          constructor = serDerClass.getConstructor(SERDE_CONSTRUCTOR_PARAM);
          serdeConstructorCache.put(className, constructor);
        }
        lineSerder = constructor.newInstance(schema, meta, targetColumnIndexes);
      } catch (Throwable e) {
        throw new RuntimeException("TextLineSerde class cannot be initialized");
      }

      return lineSerder;
    }

    public ByteBuf readLine() throws IOException {
      ByteBuf buf = reader.readLine();
      if (buf == null) {
        return null;
      } else {
        recordCount++;
      }

      return buf;
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
      try {
        if (!reader.isReadable()) return null;

        ByteBuf buf = readLine();
        if (buf == null) return null;

        if (targets.length == 0) {
          return EmptyTuple.get();
        }

        VTuple tuple = new VTuple(schema.size());
        lineSerder.buildTuple(buf, tuple);
        return tuple;
      } catch (Throwable t) {
        LOG.error("Tuple list current index: " + recordCount + " file offset:" + reader.getCompressedPosition(), t);
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
        lineSerder.release();

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
        tableStats.setNumBytes(fragment.getEndKey());
      }
      return tableStats;
    }
  }
}
