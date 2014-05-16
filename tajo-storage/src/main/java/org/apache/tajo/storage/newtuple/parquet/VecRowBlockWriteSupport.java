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

package org.apache.tajo.storage.newtuple.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.storage.newtuple.VecRowBlock;
import org.apache.tajo.storage.parquet.TajoSchemaConverter;
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.Type;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class VecRowBlockWriteSupport extends WriteSupport<VecRowBlock> {
  private RecordConsumer recordConsumer;
  private MessageType rootSchema;
  private Schema rootTajoSchema;

  private int columnNum;
  private Type [] fieldTypes;
  private TajoDataTypes.Type [] tajoDataTypes;

  /**
   * Creates a new TajoWriteSupport.
   *
   * @param tajoSchema The Tajo schema for the table.
   */
  public VecRowBlockWriteSupport(Schema tajoSchema) {
    this.rootSchema = new TajoSchemaConverter().convert(tajoSchema);
    this.rootTajoSchema = tajoSchema;

    this.columnNum = this.rootTajoSchema.size();
    fieldTypes = new Type[rootSchema.getFieldCount()];
    for (int i = 0; i < rootSchema.getFieldCount(); i++) {
      fieldTypes[i] = rootSchema.getType(i);
    }
    tajoDataTypes = new TajoDataTypes.Type[this.rootTajoSchema.size()];
    for (int i = 0; i < rootTajoSchema.size(); i++) {
      tajoDataTypes[i] = rootTajoSchema.getColumn(i).getDataType().getType();
    }
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
   * @param vecRowBlock Vectorized Row Block to write to the file.
   */
  @Override
  public void write(VecRowBlock vecRowBlock) {
    ByteBuffer varLenBuf = ByteBuffer.allocateDirect(Short.MAX_VALUE);
    for (int rowIdx = 0; rowIdx < vecRowBlock.getVectorSize(); rowIdx++) {
      recordConsumer.startMessage();
      for (int columnIdx = 0; columnIdx < columnNum; ++columnNum) {
        recordConsumer.startField(fieldTypes[columnIdx].getName(), columnIdx);
        switch (tajoDataTypes[columnIdx]) {
        case BOOLEAN:
          recordConsumer.addBoolean(vecRowBlock.getBool(columnIdx, rowIdx) == 1);
          break;
        case BIT:
        case INT2:
        case INT4:
          recordConsumer.addInteger(vecRowBlock.getInt4(columnIdx, rowIdx));
          break;
        case INT8:
          recordConsumer.addLong(vecRowBlock.getInt8(columnIdx, rowIdx));
          break;
        case FLOAT4:
          recordConsumer.addFloat(vecRowBlock.getFloat4(columnIdx, rowIdx));
          break;
        case FLOAT8:
          recordConsumer.addDouble(vecRowBlock.getInt8(columnIdx, rowIdx));
          break;
        case CHAR:
        case TEXT:
          vecRowBlock.getText(columnIdx, rowIdx, varLenBuf);
          varLenBuf.flip();
          recordConsumer.addBinary(Binary.fromByteBuffer(varLenBuf));
          break;
        case PROTOBUF:
        case BLOB:
        case INET4:
        case INET6:
          vecRowBlock.getText(columnIdx, rowIdx, varLenBuf);
          varLenBuf.flip();
          recordConsumer.addBinary(Binary.fromByteBuffer(varLenBuf));
          break;
        default:
          break;
        }

        recordConsumer.endField(fieldTypes[columnIdx].getName(), columnIdx);
      }
    }

    recordConsumer.endMessage();
  }
}
