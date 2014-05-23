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

package org.apache.tajo.engine.vector;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.storage.vector.VecRowBlock;

public class NewCukcooHashTable {

  private final static long INITIAL_BUCKET_SIZE = 2 << 10;
  private long MOD_MASK = INITIAL_BUCKET_SIZE - 1;
  private long INITIAL_PARTITION_NUMBER = 2;
  private long [] partitions;
  private long consumedMemorySize;

  public interface AggOperator {

  }

  public NewCukcooHashTable(Schema entrySchema, AggOperator op) {

  }

  public boolean isMemoryExhausted(long partition) {
    return true;
  }

  public void processPartition(long partition) {
    lookup(0, 0, 0);
  }

  public long chooseLargestPartition() {
    return 0;
  }

  public void insertPartition(long p) {

  }

  public void freePartition(long p) {

  }

  public boolean noMoreBuffers() {
    return true;
  }

  public void addBuffers() {

  }

  public void insert(VecRowBlock vector) {
    // partition id from hash % mod
    long p = 0;

    while(true) {
      if (isMemoryExhausted(p)) {
        if (noMoreBuffers()) {
          long lp = chooseLargestPartition();
          processPartition(lp);
          freePartition(lp);
        } else {
          addBuffers();
        }
        insertPartition(p);
      } else {
        insertPartition(p);
      }
    }
  }

  public void flushPartition() {
    for (int p = 0; p < 0; p++) {
      processPartition(p);
      freePartition(p);
    }
  }

  public void lookup(int num, long outputPtr, long inputPtr) {

  }


  public void free() {

  }
}
