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

/**
 * 
 */
package org.apache.tajo.storage.index;

import org.apache.tajo.storage.Tuple;

import java.io.IOException;

public interface OrderIndexReader extends IndexReader {
  /**
   * Find the offset corresponding to key which is equal to or greater than 
   * a given key.
   * 
   * @param key to find
   * @return
   * @throws java.io.IOException
   */
  public long find(Tuple key, boolean nextKey) throws IOException;

  /**
   * Return the next offset from the latest find or next offset
   * @return
   * @throws java.io.IOException
   */
  public long next() throws IOException;
}
