/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.tuple.memory;

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.tuple.RowBlockReader;

import java.io.IOException;
import java.nio.channels.ScatteringByteChannel;

public interface RowBlock {
  /**
   * a data format for de/serialization
   */
  String getDataFormat();

  /**
   * reset the memory and writer
   */
  void clear();

  /**
   * @return the number of bytes this memory block can contain.
   */
  int capacity();

  /**
   * @return the number of written bytes in this memory block
   */
  int usedMem();

  /**
   * @return the percentage of written bytes in this memory block
   */
  float usage();

  void setRows(int rowNum);

  int rows();

  TajoDataTypes.DataType[] getDataTypes();

  RowBlockReader getReader();

  RowWriter getWriter();

  MemoryBlock getMemory();

  void release();

  boolean copyFromChannel(ScatteringByteChannel channel) throws IOException;
}
