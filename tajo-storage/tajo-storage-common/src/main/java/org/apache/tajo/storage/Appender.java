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

package org.apache.tajo.storage;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.statistics.TableStats;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 *
 * Interface for appender.
 * Every appender that writes some data to underlying storage needs to implement this interface.
 */
public interface Appender extends Closeable {

  /**
   * Initialize the appender.
   *
   * @throws IOException
   */
  void init() throws IOException;

  /**
   * Write the given tuple.
   *
   * @param t
   * @throws IOException
   */
  void addTuple(Tuple t) throws IOException;

  /**
   * Flush buffered tuples if they exist.
   *
   * @throws IOException
   */
  void flush() throws IOException;

  /**
   * The total size of written output.
   * The result value can be different from the real size due to the buffered data.
   *
   * @return
   * @throws IOException
   */
  long getEstimatedOutputSize() throws IOException;

  /**
   * Close the appender.
   *
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Enable statistics collection for the output table.
   */
  void enableStats();

  /**
   * Enable statistics collection for the output table as well as its columns.
   * Note that statistics are collected on only the specified columns.
   * @param columnList a list of columns on which statistics is collected
   */
  void enableStats(List<Column> columnList);

  /**
   * Return collected statistics.
   *
   * @return statistics
   */
  TableStats getStats();
}
