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

import java.io.IOException;

/**
 * Read/write unit
 */
public interface Block {

  void setMeta(BlockMeta meta);

  /**
   * Get the metadata of the block
   *
   * @return the metadata
   */
  BlockMeta getMeta() throws IOException;

  /**
   * Return the whole values as an array.
   *
   * @return the array of values in the block
   */
  Datum[] asArray() throws IOException;

  /**
   * Return a datum of the current row pointed by the cursor,
   * and advance the cursor
   *
   * @return retrieve null if the scanner has no more datum.
   * Otherwise it returns one datum.
   */
  Datum next() throws IOException;

  /**
   * Get the position of the cursor.
   *
   * @return the position of the value which is currently pointed by the cursor
   */
  int getPos() throws IOException;

  /**
   * Return the number of bytes of data in the block.
   *
   * @return the number of bytes
   * @throws IOException
   */
  int getSize() throws IOException;

  int getDataNum() throws IOException;
}
