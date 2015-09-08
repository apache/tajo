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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.thirdparty.parquet.ParquetReader;
import parquet.filter.UnboundRecordFilter;

import java.io.IOException;

/**
 * Tajo implementation of {@link ParquetReader} to read Tajo records from a
 * Parquet file. Users should use {@link ParquetScanner} and not this class
 * directly.
 */
public class TajoParquetReader extends ParquetReader<Tuple> {
  /**
   * Creates a new TajoParquetReader.
   *
   * @param file The file to read from.
   * @param readSchema Tajo schema of the table.
   */
  public TajoParquetReader(Path file, Schema readSchema) throws IOException {
    super(file, new TajoReadSupport(readSchema));
  }

  /**
   * Creates a new TajoParquetReader.
   *
   * @param file The file to read from.
   * @param readSchema Tajo schema of the table.
   * @param requestedSchema Tajo schema of the projection.
   */
  public TajoParquetReader(Path file, Schema readSchema,
                           Schema requestedSchema) throws IOException {
    super(file, new TajoReadSupport(readSchema, requestedSchema));
  }

  /**
   * Creates a new TajoParquetReader.
   *
   * @param file The file to read from.
   * @param readSchema Tajo schema of the table.
   * @param recordFilter Record filter.
   */
  public TajoParquetReader(Path file, Schema readSchema,
                           UnboundRecordFilter recordFilter)
      throws IOException {
    super(file, new TajoReadSupport(readSchema), recordFilter);
  }

  /**
   * Creates a new TajoParquetReader.
   *
   * @param file The file to read from.
   * @param readSchema Tajo schema of the table.
   * @param requestedSchema Tajo schema of the projection.
   * @param recordFilter Record filter.
   */
  public TajoParquetReader(Path file, Schema readSchema,
                           Schema requestedSchema,
                           UnboundRecordFilter recordFilter)
      throws IOException {
    super(file, new TajoReadSupport(readSchema, requestedSchema),
          recordFilter);
  }
}
