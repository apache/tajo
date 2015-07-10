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

package org.apache.tajo.storage.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.storage.FileAppender;
import org.apache.tajo.storage.TableStatistics;
import org.apache.tajo.storage.Tuple;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * FileAppender for writing to Avro files.
 */
public class AvroAppender extends FileAppender {
  private TableStatistics stats;
  private Schema avroSchema;
  private List<Schema.Field> avroFields;
  private DataFileWriter<GenericRecord> dataFileWriter;

  /**
   * Creates a new AvroAppender.
   *
   * @param conf Configuration properties.
   * @param taskAttemptId The task attempt id
   * @param schema The table schema.
   * @param meta The table metadata.
   * @param workDir The path of the Parquet file to write to.
   */
  public AvroAppender(Configuration conf,
                      TaskAttemptId taskAttemptId,
                      org.apache.tajo.catalog.Schema schema,
                      TableMeta meta, Path workDir) throws IOException {
    super(conf, taskAttemptId, schema, meta, workDir);
  }

  /**
   * Initializes the Appender.
   */
  public void init() throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (!fs.exists(path.getParent())) {
      throw new FileNotFoundException(path.toString());
    }
    FSDataOutputStream outputStream = fs.create(path);

    avroSchema = AvroUtil.getAvroSchema(meta, conf);
    avroFields = avroSchema.getFields();

    DatumWriter<GenericRecord> datumWriter =
        new GenericDatumWriter<GenericRecord>(avroSchema);
    dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(avroSchema, outputStream);

    if (enabledStats) {
      this.stats = new TableStatistics(schema);
    }
    super.init();
  }

  /**
   * Gets the current offset. Tracking offsets is currently not implemented, so
   * this method always returns 0.
   *
   * @return 0
   */
  @Override
  public long getOffset() throws IOException {
    return 0;
  }

  private Object getPrimitive(Tuple tuple, int i, Schema.Type avroType) {
    if (tuple.isBlankOrNull(i)) {
      return null;
    }
    switch (avroType) {
      case NULL:
        return null;
      case BOOLEAN:
        return tuple.getBool(i);
      case INT:
        return tuple.getInt4(i);
      case LONG:
        return tuple.getInt8(i);
      case FLOAT:
        return tuple.getFloat4(i);
      case DOUBLE:
        return tuple.getFloat8(i);
      case BYTES:
      case FIXED:
        return ByteBuffer.wrap(tuple.getBytes(i));
      case STRING:
        return tuple.getText(i);
      default:
        throw new RuntimeException("Unknown primitive type.");
    }
  }

  /**
   * Write a Tuple to the Avro file.
   *
   * @param tuple The Tuple to write.
   */
  @Override
  public void addTuple(Tuple tuple) throws IOException {
    GenericRecord record = new GenericData.Record(avroSchema);
    for (int i = 0; i < schema.size(); ++i) {
      Column column = schema.getColumn(i);
      if (enabledStats) {
        stats.analyzeField(i, tuple);
      }
      Object value;
      Schema.Field avroField = avroFields.get(i);
      Schema.Type avroType = avroField.schema().getType();
      switch (avroType) {
        case NULL:
        case BOOLEAN:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BYTES:
        case STRING:
        case FIXED:
          value = getPrimitive(tuple, i, avroType);
          break;
        case RECORD:
          throw new RuntimeException("Avro RECORD not supported.");
        case ENUM:
          throw new RuntimeException("Avro ENUM not supported.");
        case MAP:
          throw new RuntimeException("Avro MAP not supported.");
        case UNION:
          List<Schema> schemas = avroField.schema().getTypes();
          if (schemas.size() != 2) {
            throw new RuntimeException("Avro UNION not supported.");
          }
          if (schemas.get(0).getType().equals(Schema.Type.NULL)) {
            value = getPrimitive(tuple, i, schemas.get(1).getType());
          } else if (schemas.get(1).getType().equals(Schema.Type.NULL)) {
            value = getPrimitive(tuple, i, schemas.get(0).getType());
          } else {
            throw new RuntimeException("Avro UNION not supported.");
          }
          break;
        default:
          throw new RuntimeException("Unknown type: " + avroType);
      }
      record.put(i, value);
    }
    dataFileWriter.append(record);

    if (enabledStats) {
      stats.incrementRow();
    }
  }

  /**
   * Flushes the current state of the file.
   */
  @Override
  public void flush() throws IOException {
    dataFileWriter.flush();
  }

  /**
   * Closes the Appender.
   */
  @Override
  public void close() throws IOException {
    IOUtils.cleanup(null, dataFileWriter);
  }

  /**
   * If table statistics is enabled, retrieve the table statistics.
   *
   * @return Table statistics if enabled or null otherwise.
   */
  @Override
  public TableStats getStats() {
    if (enabledStats) {
      return stats.getTableStat();
    } else {
      return null;
    }
  }
}
