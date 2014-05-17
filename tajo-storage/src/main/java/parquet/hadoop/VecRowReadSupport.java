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

package parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.storage.parquet.TajoSchemaConverter;
import parquet.Log;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

import java.util.Map;

/**
 * Tajo implementation of {@link parquet.hadoop.api.ReadSupport} for {@link org.apache.tajo.storage.Tuple}s.
 * Users should use {@link org.apache.tajo.storage.parquet.ParquetScanner} and not this class directly.
 */
public class VecRowReadSupport extends ReadSupport<Object []> {
  private static final Log LOG = Log.getLog(VecRowReadSupport.class);

  private Schema readSchema;
  private Schema requestedSchema;

  /**
   * Creates a new TajoReadSupport.
   *
   * @param requestedSchema The Tajo schema of the requested projection passed
   *        down by ParquetScanner.
   */
  public VecRowReadSupport(Schema readSchema, Schema requestedSchema) {
    super();
    this.readSchema = readSchema;
    this.requestedSchema = requestedSchema;
  }

  /**
   * Creates a new TajoReadSupport.
   *
   * @param readSchema The schema of the table.
   */
  public VecRowReadSupport(Schema readSchema) {
    super();
    this.readSchema = readSchema;
    this.requestedSchema = readSchema;
  }

  /**
   * Initializes the ReadSupport.
   *
   * @param context The InitContext.
   * @return A ReadContext that defines how to read the file.
   */
  @Override
  public ReadContext init(InitContext context) {
    if (requestedSchema == null) {
      throw new RuntimeException("requestedSchema is null.");
    }
    MessageType requestedParquetSchema =
      new TajoSchemaConverter().convert(requestedSchema);
    LOG.debug("Reading data with projection:\n" + requestedParquetSchema);
    return new ReadContext(requestedParquetSchema);
  }

  /**
   * Prepares for read.
   *
   * @param configuration The job configuration.
   * @param keyValueMetaData App-specific metadata from the file.
   * @param fileSchema The schema of the Parquet file.
   * @param readContext Returned by the init method.
   */
  @Override
  public RecordMaterializer<Object []> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext) {
    MessageType parquetRequestedSchema = readContext.getRequestedSchema();
    return new VecRowMaterializer(parquetRequestedSchema, requestedSchema, readSchema);
  }

  public Schema getSchema() {
    return readSchema;
  }

  public Schema getTargetSchema() {
    return requestedSchema;
  }
}
