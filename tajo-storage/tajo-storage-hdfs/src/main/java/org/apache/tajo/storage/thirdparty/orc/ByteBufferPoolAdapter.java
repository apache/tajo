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

package org.apache.tajo.storage.thirdparty.orc;

import org.apache.hadoop.io.ByteBufferPool;

import java.nio.ByteBuffer;

public class ByteBufferPoolAdapter implements ByteBufferPool {
  private ByteBufferAllocatorPool pool;

  public ByteBufferPoolAdapter(ByteBufferAllocatorPool pool) {
    this.pool = pool;
  }

  @Override
  public final ByteBuffer getBuffer(boolean direct, int length) {
    return this.pool.getBuffer(direct, length);
  }
  
  @Override
  public final void putBuffer(ByteBuffer buffer) {
    this.pool.putBuffer(buffer);
  }
}
