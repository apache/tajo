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
import org.apache.tajo.catalog.SchemaObject;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;

import java.io.Closeable;
import java.io.IOException;

/**
 * Scanner Interface
 */

public interface Scanner extends SchemaObject, Closeable {
  void init() throws IOException;

  /**
   * It returns one tuple at each call. 
   * 
   * @return retrieve null if the scanner has no more tuples. 
   * Otherwise it returns one tuple.
   * 
   * @throws java.io.IOException if internal I/O error occurs during next method
   */
  Tuple next() throws IOException;

  /**
   * Reset the cursor. After executed, the scanner
   * will retrieve the first tuple.
   *
   * @throws java.io.IOException if internal I/O error occurs during reset method
   */
  void reset() throws IOException;

  /**
   * Close scanner
   *
   * @throws java.io.IOException if internal I/O error occurs during close method
   */
  void close() throws IOException;

  /**
   * Push a plan part into scanner. It will be used in future issues.
   *
   * @param planPart
   */
  void pushOperators(LogicalNode planPart);

  /**
   * It returns if the projection is executed in the underlying scanner layer.
   *
   * If TRUE, the upper layers (i.e., SeqScanExec) assume that next()
   * will return a tuple which contains only projected fields. In other words,
   * the field number of a retrieved tuple is equivalent tothe number of targets.
   *
   * If FALSE, the upper layers assume that next() will return a tuple which
   * contains projected fields and non-projected fields, padded by NullDatum.
   * In other words, the width of tuple is equivalent to the field number
   * of the table schema.
   *
   * @return true if this scanner can project the given columns.
   */
  boolean isProjectable();

  /**
   * Set target columns
   * @param targets columns to be projected
   */
  void setTarget(Column[] targets);

  /**
   * It returns if the selection is executed in the underlying scanner layer.
   *
   * @return true if this scanner can filter tuples against a given condition.
   */
  boolean isSelectable();

  /**
   * Set a filter condition
   * @param filter to be searched
   *
   * TODO - to be changed Object type
   */
  void setFilter(EvalNode filter);


  /**
   * This method does not guarantee that the scanner will retrieve the specified number of rows.
   * This information is used for a hint.
   *
   * @param num The number of rows to be retrieved.
   */
  void setLimit(long num);

  /**
   * It returns if the file is splittable.
   *
   * @return true if this scanner can split the a file.
   */
  boolean isSplittable();

  /**
   * How much of the input has the Scanner consumed
   * @return progress from <code>0.0</code> to <code>1.0</code>.
   */
  float getProgress();

  TableStats getInputStats();
}
