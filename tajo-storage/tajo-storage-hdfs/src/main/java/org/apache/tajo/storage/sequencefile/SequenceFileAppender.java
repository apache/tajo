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

package org.apache.tajo.storage.sequencefile;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.exception.AlreadyExistsStorageException;
import org.apache.tajo.storage.rcfile.NonSyncByteArrayOutputStream;
import org.apache.tajo.util.BytesUtils;

import java.io.IOException;

public class SequenceFileAppender extends FileAppender {
  private static final Log LOG = LogFactory.getLog(SequenceFileScanner.class);

  private SequenceFile.Writer writer;

  private TableMeta meta;
  private Schema schema;
  private TableStatistics stats = null;

  private int columnNum;
  private FileSystem fs;
  private char delimiter;
  private byte[] nullChars;

  private final static int BUFFER_SIZE = 128 * 1024;
  private long pos = 0;

  private CompressionCodecFactory codecFactory;
  private CompressionCodec codec;

  private NonSyncByteArrayOutputStream os;
  private SerializerDeserializer serde;

  long rowCount;
  private boolean isShuffle;

  private Writable EMPTY_KEY;

  public SequenceFileAppender(Configuration conf, TaskAttemptId taskAttemptId,
                              Schema schema, TableMeta meta, Path workDir) throws IOException {
    super(conf, taskAttemptId, schema, meta, workDir);
    this.meta = meta;
    this.schema = schema;
  }

  @Override
  public void init() throws IOException {
    os = new NonSyncByteArrayOutputStream(BUFFER_SIZE);

    this.fs = path.getFileSystem(conf);

    this.delimiter = StringEscapeUtils.unescapeJava(this.meta.getOption(StorageConstants.SEQUENCEFILE_DELIMITER,
        StorageConstants.DEFAULT_FIELD_DELIMITER)).charAt(0);
    this.columnNum = schema.size();
    String nullCharacters = StringEscapeUtils.unescapeJava(this.meta.getOption(StorageConstants.SEQUENCEFILE_NULL,
        NullDatum.DEFAULT_TEXT));
    if (StringUtils.isEmpty(nullCharacters)) {
      nullChars = NullDatum.get().asTextBytes();
    } else {
      nullChars = nullCharacters.getBytes();
    }

    if(this.meta.containsOption(StorageConstants.COMPRESSION_CODEC)) {
      String codecName = this.meta.getOption(StorageConstants.COMPRESSION_CODEC);
      codecFactory = new CompressionCodecFactory(conf);
      codec = codecFactory.getCodecByClassName(codecName);
    } else {
      if (fs.exists(path)) {
        throw new AlreadyExistsStorageException(path);
      }
    }

    try {
      String serdeClass = this.meta.getOption(StorageConstants.SEQUENCEFILE_SERDE,
          TextSerializerDeserializer.class.getName());
      serde = (SerializerDeserializer) Class.forName(serdeClass).newInstance();
      serde.init(schema);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e);
    }

    Class<? extends Writable>  keyClass, valueClass;
    if (serde instanceof BinarySerializerDeserializer) {
      keyClass = BytesWritable.class;
      EMPTY_KEY = new BytesWritable();
      valueClass = BytesWritable.class;
    } else {
      keyClass = LongWritable.class;
      EMPTY_KEY = new LongWritable();
      valueClass = Text.class;
    }

    String type = this.meta.getOption(StorageConstants.COMPRESSION_TYPE, CompressionType.NONE.name());
    if (type.equals(CompressionType.BLOCK.name())) {
      writer = SequenceFile.createWriter(fs, conf, path, keyClass, valueClass, CompressionType.BLOCK, codec);
    } else if (type.equals(CompressionType.RECORD.name())) {
      writer = SequenceFile.createWriter(fs, conf, path, keyClass, valueClass, CompressionType.RECORD, codec);
    } else {
      writer = SequenceFile.createWriter(fs, conf, path, keyClass, valueClass, CompressionType.NONE, codec);
    }

    if (tableStatsEnabled) {
      this.stats = new TableStatistics(this.schema, columnStatsEnabled);
    }

    super.init();
  }

  @Override
  public void addTuple(Tuple tuple) throws IOException {

    if (serde instanceof BinarySerializerDeserializer) {
      byte nullByte = 0;
      int lasti = 0;
      for (int i = 0; i < columnNum; i++) {
        // set bit to 1 if a field is not null
        if (!tuple.isBlank(i)) {
          nullByte |= 1 << (i % 8);
        }

        // write the null byte every eight elements or
        // if this is the last element and serialize the
        // corresponding 8 struct fields at the same time
        if (7 == i % 8 || i == columnNum - 1) {
          os.write(nullByte);

          for (int j = lasti; j <= i; j++) {

            switch (schema.getColumn(j).getDataType().getType()) {
              case TEXT:
                BytesUtils.writeVLong(os, tuple.getTextBytes(j).length);
                break;
              case PROTOBUF:
                ProtobufDatum protobufDatum = (ProtobufDatum) tuple.getProtobufDatum(j);
                BytesUtils.writeVLong(os, protobufDatum.asByteArray().length);
                break;
              case CHAR:
              case INET4:
              case BLOB:
                BytesUtils.writeVLong(os, tuple.getBytes(j).length);
                break;
              default:
            }

            serde.serialize(j, tuple, os, nullChars);
          }
          lasti = i + 1;
          nullByte = 0;
        }
      }

      BytesWritable b = new BytesWritable();
      b.set(os.getData(), 0, os.getLength());
      writer.append(EMPTY_KEY, b);

    } else {
      for (int i = 0; i < columnNum; i++) {
        serde.serialize(i, tuple, os, nullChars);

        if (columnNum -1 > i) {
          os.write((byte) delimiter);
        }
      }
      writer.append(EMPTY_KEY, new Text(os.toByteArray()));
    }

    os.reset();
    pos += writer.getLength();
    rowCount++;

    if (tableStatsEnabled) {
      stats.incrementRow();
    }
  }

  @Override
  public long getOffset() throws IOException {
    return pos;
  }

  @Override
  public void flush() throws IOException {
    os.flush();
  }

  @Override
  public void close() throws IOException {
    // Statistical section
    if (tableStatsEnabled) {
      stats.setNumBytes(getOffset());
    }

    IOUtils.cleanup(LOG, os, writer);
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
