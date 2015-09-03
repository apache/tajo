/*
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

package org.apache.tajo.tuple.memory;

import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.storage.Tuple;

public abstract class ZeroCopyTuple implements Tuple {

  protected int relativePos;
  protected int length;

  public abstract void set(MemoryBlock memoryBlock, int relativePos, int length, DataType[] types);

  void set(int relativePos, int length) {
    this.relativePos = relativePos;
    this.length = length;
  }

  public int getRelativePos() {
    return relativePos;
  }

  public int getLength() {
    return length;
  }

  @Override
  public Tuple clone() throws CloneNotSupportedException {
    return (Tuple) super.clone();
  }
}
