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

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.storage.Tuple;
import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

/**
 * Materializes a Tajo Tuple from a stream of Parquet data.
 */
class TajoRecordMaterializer extends RecordMaterializer<Tuple> {
  private final TajoRecordConverter root;

  /**
   * Creates a new TajoRecordMaterializer.
   *
   * @param parquetSchema The Parquet schema of the projection.
   * @param tajoSchema The Tajo schema of the projection.
   * @param tajoReadSchema The Tajo schema of the table.
   */
  public TajoRecordMaterializer(MessageType parquetSchema, Schema tajoSchema,
                                Schema tajoReadSchema) {
    int[] projectionMap = getProjectionMap(tajoReadSchema, tajoSchema);
    this.root = new TajoRecordConverter(parquetSchema, tajoReadSchema,
                                        projectionMap);
  }

  private int[] getProjectionMap(Schema schema, Schema projection) {
    Column[] targets = projection.toArray();
    int[] projectionMap = new int[targets.length];
    for (int i = 0; i < targets.length; ++i) {
      int tid = schema.getColumnId(targets[i].getQualifiedName());
      projectionMap[i] = tid;
    }
    return projectionMap;
  }

  /**
   * Returns the current record being materialized.
   *
   * @return The record being materialized.
   */
  @Override
  public Tuple getCurrentRecord() {
    return root.getCurrentRecord();
  }

  /**
   * Returns the root converter.
   *
   * @return The root converter
   */
  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}
