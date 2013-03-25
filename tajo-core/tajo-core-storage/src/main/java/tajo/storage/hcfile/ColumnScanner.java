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

package tajo.storage.hcfile;

import tajo.datum.Datum;

import java.io.Closeable;
import java.io.IOException;

/**
 * Scanner interface for columnar files
 */
public interface ColumnScanner extends Closeable, Seekable {

  ColumnMeta getMeta() throws IOException;

  /**
   * Move the cursor to the first row.
   * After this is called, get() retrieves the first datum.
   *
   * @throws IOException if internal I/O error occurs during reset method
   */
  @Override
  void first() throws IOException;

  /**
   * Move the cursor to the last row.
   * After this is called, get() retrieves the last datum.
   *
   * @throws IOException if internal I/O error occurs during reset method
   */
  @Override
  void last() throws IOException;

  /**
   * Return a datum of the current row pointed by the cursor,
   * and advance the cursor
   *
   * @return retrieve null if the scanner has no more datum.
   * Otherwise it returns one datum.
   *
   * @throws IOException if internal I/O error occurs during get method
   */
  Datum get() throws IOException;

  /**
   * Move the cursor to the given position
   *
   * @param rid indicates the desired position
   * @throws IOException if the desired position is larger than the max row id
   */
  @Override
  void pos(long rid) throws IOException;

  /**
   * Retrieves the get block
   *
   * @return retrieve null if the scanner has no more block.
   * Otherwise it returns one block.
   *
   * @throws IOException if internal I/O error occurs during reset method
   */
  Block getBlock() throws IOException;

  /**
   * Returns an array of datums in the get block.
   *
   * @return retrieve null if the scanner has no more block.
   * Otherwise it returns an array of datums in the get block.
   *
   * @throws IOException if internal I/O error occurs during get method
   */
  Datum[] getBlockAsArray() throws IOException;


  /**
   * Return the id of the current row.
   *
   * @return the id of the current row
   * @throws IOException if internal I/O error occurs during get method
   */
  long getPos() throws IOException;

  /**
   * Close scanner
   *
   * @throws IOException if internal I/O error occurs during close method
   */
  @Override
  void close() throws IOException;
}
