/***
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

package org.apache.tajo.storage.rowblock;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SchemaObject;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.directmem.VecRowBlock;

import java.io.Closeable;
import java.io.IOException;

/**
 * RowBlock Scanner Interface
 */
public interface RowBlockScanner {

  /**
   * It returns one tuple at each call.
   *
   * @return retrieve null if the scanner has no more tuples.
   * Otherwise it returns one tuple.
   *
   * @throws java.io.IOException if internal I/O error occurs during next method
   */
  boolean next(VecRowBlock rowBlock) throws IOException;
}
