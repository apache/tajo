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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.exception.AlreadyExistsStorageException;
import org.apache.tajo.storage.rcfile.NonSyncByteArrayOutputStream;
import org.apache.tajo.util.BytesUtils;

import java.io.FileNotFoundException;
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
  private static final BytesWritable EMPTY_KEY = new BytesWritable();

  public SequenceFileAppender(Configuration conf, Schema schema, TableMeta meta, Path path) throws IOException {
    super(conf, schema, meta, path);
    this.meta = meta;
    this.schema = schema;
  }

  @Override
  public void init() throws IOException {
    os = new NonSyncByteArrayOutputStream(BUFFER_SIZE);

    this.fs = path.getFileSystem(conf);

    //determine the intermediate file type
    String store = conf.get(TajoConf.ConfVars.SHUFFLE_FILE_FORMAT.varname,
        TajoConf.ConfVars.SHUFFLE_FILE_FORMAT.defaultVal);
    if (enabledStats && CatalogProtos.StoreType.SEQUENCEFILE == CatalogProtos.StoreType.valueOf(store.toUpperCase())) {
      isShuffle = true;
    } else {
      isShuffle = false;
    }

    this.delimiter = StringEscapeUtils.unescapeJava(this.meta.getOption(StorageConstants.SEQUENCEFILE_DELIMITER,
        StorageConstants.DEFAULT_FIELD_DELIMITER)).charAt(0);
    this.columnNum = schema.size();
    String nullCharacters = StringEscapeUtils.unescapeJava(this.meta.getOption(StorageConstants.SEQUENCEFILE_NULL));
    if (StringUtils.isEmpty(nullCharacters)) {
      nullChars = NullDatum.get().asTextBytes();
    } else {
      nullChars = nullCharacters.getBytes();
    }

    if (!fs.exists(path.getParent())) {
      throw new FileNotFoundException(path.toString());
    }

    String codecName = this.meta.getOption(StorageConstants.COMPRESSION_CODEC);
    if(!StringUtils.isEmpty(codecName)){
      codecFactory = new CompressionCodecFactory(conf);
      codec = codecFactory.getCodecByClassName(codecName);
    } else {
      if (fs.exists(path)) {
        throw new AlreadyExistsStorageException(path);
      }
    }

    try {
      String serdeClass = this.meta.getOption(StorageConstants.SEQUENCEFILE_SERDE, TextSerializerDeserializer.class.getName());
      serde = (SerializerDeserializer) Class.forName(serdeClass).newInstance();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e);
    }

    Class<? extends Writable>  valueClass;
    if (serde instanceof BinarySerializerDeserializer) {
      valueClass = BytesWritable.class;
    } else {
      valueClass = Text.class;
    }

    String type = this.meta.getOption(StorageConstants.COMPRESSION_TYPE, CompressionType.NONE.name());
    if (type.equals(CompressionType.BLOCK.name())) {
      writer = SequenceFile.createWriter(fs, conf, path, BytesWritable.class, valueClass, CompressionType.BLOCK, codec);
    } else if (type.equals(CompressionType.RECORD.name())) {
      writer = SequenceFile.createWriter(fs, conf, path, BytesWritable.class, valueClass, CompressionType.RECORD, codec);
    } else {
      writer = SequenceFile.createWriter(fs, conf, path, BytesWritable.class, valueClass, CompressionType.NONE, codec);
    }

    if (enabledStats) {
      this.stats = new TableStatistics(this.schema);
    }

    super.init();
  }

  @Override
  public void addTuple(Tuple tuple) throws IOException {
    Datum datum;

    if (serde instanceof BinarySerializerDeserializer) {
      byte nullByte = 0;
      int lasti = 0;
      for (int i = 0; i < columnNum; i++) {
        datum = tuple.get(i);

        // set bit to 1 if a field is not null
        if (null != datum) {
          nullByte |= 1 << (i % 8);
        }

        // write the null byte every eight elements or
        // if this is the last element and serialize the
        // corresponding 8 struct fields at the same time
        if (7 == i % 8 || i == columnNum - 1) {
          os.write(nullByte);

          for (int j = lasti; j <= i; j++) {
            datum = tuple.get(j);

            switch (schema.getColumn(j).getDataType().getType()) {
              case TEXT:
                BytesUtils.writeVLong(os, datum.asTextBytes().length);
                break;
              case PROTOBUF:
                ProtobufDatum protobufDatum = (ProtobufDatum) datum;
                BytesUtils.writeVLong(os, protobufDatum.asByteArray().length);
                break;
              case CHAR:
              case INET4:
              case BLOB:
                BytesUtils.writeVLong(os, datum.asByteArray().length);
                break;
              default:
            }

            serde.serialize(schema.getColumn(j), datum, os, nullChars);

            if (isShuffle) {
              // it is to calculate min/max values, and it is only used for the intermediate file.
              stats.analyzeField(j, datum);
            }
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
        datum = tuple.get(i);
        serde.serialize(schema.getColumn(i), datum, os, nullChars);

        if (columnNum -1 > i) {
          os.write((byte) delimiter);
        }

        if (isShuffle) {
          // it is to calculate min/max values, and it is only used for the intermediate file.
          stats.analyzeField(i, datum);
        }

      }
      writer.append(EMPTY_KEY, new Text(os.toByteArray()));
    }

    os.reset();
    pos += writer.getLength();
    rowCount++;

    if (enabledStats) {
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
    writer.close();
  }

  @Override
  public void close() throws IOException {
    // Statistical section
    if (enabledStats) {
      stats.setNumBytes(getOffset());
    }

    os.close();
    writer.close();
  }

  @Override
  public TableStats getStats() {
    if (enabledStats) {
      return stats.getTableStat();
    } else {
      return null;
    }
  }

}
