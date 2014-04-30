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

package org.apache.tajo.storage.parquet;

import java.util.Map;
import java.util.HashMap;
import java.util.List;

import parquet.hadoop.api.WriteSupport;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.datum.Datum;

/**
 * Tajo implementation of {@link WriteSupport} for {@link Tuple}s.
 * Users should use {@link ParquetAppender} and not this class directly.
 */
public class TajoWriteSupport extends WriteSupport<Tuple> {
  private RecordConsumer recordConsumer;
  private MessageType rootSchema;
  private Schema rootTajoSchema;

  /**
   * Creates a new TajoWriteSupport.
   *
   * @param tajoSchema The Tajo schema for the table.
   */
  public TajoWriteSupport(Schema tajoSchema) {
    this.rootSchema = new TajoSchemaConverter().convert(tajoSchema);
    this.rootTajoSchema = tajoSchema;
  }

  /**
   * Initializes the WriteSupport.
   *
   * @param configuration The job's configuration.
   * @return A WriteContext that describes how to write the file.
   */
  @Override
  public WriteContext init(Configuration configuration) {
    Map<String, String> extraMetaData = new HashMap<String, String>();
    return new WriteContext(rootSchema, extraMetaData);
  }

  /**
   * Called once per row group.
   *
   * @param recordConsumer The {@link RecordConsumer} to write to.
   */
  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  /**
   * Writes a Tuple to the file.
   *
   * @param tuple The Tuple to write to the file.
   */
  @Override
  public void write(Tuple tuple) {
    recordConsumer.startMessage();
    writeRecordFields(rootSchema, rootTajoSchema, tuple);
    recordConsumer.endMessage();
  }

  private void writeRecordFields(GroupType schema, Schema tajoSchema,
                                 Tuple tuple) {
    List<Type> fields = schema.getFields();
    // Parquet ignores Tajo NULL_TYPE columns, so the index may differ.
    int index = 0;
    for (int tajoIndex = 0; tajoIndex < tajoSchema.size(); ++tajoIndex) {
      Column column = tajoSchema.getColumn(tajoIndex);
      if (column.getDataType().getType() == TajoDataTypes.Type.NULL_TYPE) {
        continue;
      }
      Datum datum = tuple.get(tajoIndex);
      Type fieldType = fields.get(index);
      if (!tuple.isNull(tajoIndex)) {
        recordConsumer.startField(fieldType.getName(), index);
        writeValue(fieldType, column, datum);
        recordConsumer.endField(fieldType.getName(), index);
      } else if (fieldType.isRepetition(Type.Repetition.REQUIRED)) {
        throw new RuntimeException("Null-value for required field: " +
            column.getSimpleName());
      }
      ++index;
    }
  }

  private void writeValue(Type fieldType, Column column, Datum datum) {
    switch (column.getDataType().getType()) {
      case BOOLEAN:
        recordConsumer.addBoolean((Boolean) datum.asBool());
        break;
      case BIT:
      case INT2:
      case INT4:
        recordConsumer.addInteger(datum.asInt4());
        break;
      case INT8:
        recordConsumer.addLong(datum.asInt8());
        break;
      case FLOAT4:
        recordConsumer.addFloat(datum.asFloat4());
        break;
      case FLOAT8:
        recordConsumer.addDouble(datum.asFloat8());
        break;
      case CHAR:
      case TEXT:
        recordConsumer.addBinary(Binary.fromString(datum.asChars()));
        break;
      case PROTOBUF:
      case BLOB:
      case INET4:
      case INET6:
        recordConsumer.addBinary(Binary.fromByteArray(datum.asByteArray()));
        break;
      default:
        break;
    }
  }
}
