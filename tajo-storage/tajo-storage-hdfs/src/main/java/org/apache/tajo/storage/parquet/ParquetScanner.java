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

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.storage.EmptyTuple;
import org.apache.tajo.storage.FileScanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.Fragment;

import java.io.IOException;

/**
 * FileScanner for reading Parquet files
 */
public class ParquetScanner extends FileScanner {
  private TajoParquetReader reader;
  /** The number of actual read records */
  private long currentRowCount;
  private long totalRowCount;
  private boolean closed;

  /**
   * Creates a new ParquetScanner.
   *
   * @param conf
   * @param schema
   * @param meta
   * @param fragment
   */
  public ParquetScanner(Configuration conf, final Schema schema,
                        final TableMeta meta, final Fragment fragment) {
    super(conf, schema, meta, fragment);
  }

  /**
   * Initializes the ParquetScanner. This method initializes the
   * TajoParquetReader.
   */
  @Override
  public void init() throws IOException {
    if (targets == null) {
      targets = schema.toArray();
    }
    reader = new TajoParquetReader(conf, fragment.getPath(), schema,
        SchemaBuilder.builder().addAll(targets).build());
    totalRowCount = reader.getTotalRowCount();
    currentRowCount = 0;
    closed = false;
    super.init();
  }

  /**
   * Reads the next Tuple from the Parquet file.
   *
   * @return The next Tuple from the Parquet file or null if end of file is
   *         reached.
   */
  @Override
  public Tuple next() throws IOException {
    // If there is no required column, we just read footer and then return an empty tuple
    if (targets.length == 0) {
      if(currentRowCount == totalRowCount) {
        return null;
      } else {
        currentRowCount++;
        return EmptyTuple.get();
      }
    }

    return reader.read();
  }

  /**
   * Resets the scanner
   */
  @Override
  public void reset() throws IOException {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  /**
   * Closes the scanner.
   */
  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
    closed = true;
  }

  /**
   * Returns whether this scanner is projectable.
   *
   * @return true
   */
  @Override
  public boolean isProjectable() {
    return true;
  }

  /**
   * Returns whether this scanner is selectable.
   *
   * @return false
   */
  @Override
  public boolean isSelectable() {
    return false;
  }

  @Override
  public void setFilter(EvalNode filter) {
    throw new TajoRuntimeException(new NotImplementedException());
  }

  /**
   * Returns whether this scanner is splittable.
   *
   * @return false
   */
  @Override
  public boolean isSplittable() {
    return false;
  }

  @Override
  public float getProgress() {

    if (!inited) {
      return super.getProgress();
    } else {
      if (closed) {
        return 1.0f;
      } else {
        return reader.getProgress();
      }
    }
  }
}
