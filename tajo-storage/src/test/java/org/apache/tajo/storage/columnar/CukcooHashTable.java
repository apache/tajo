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

package org.apache.tajo.storage.columnar;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.storage.columnar.map.VecFuncMulMul3LongCol;
import org.apache.tajo.util.Pair;

/**
* Created by hyunsik on 5/21/14.
*/
public class CukcooHashTable<V> {
  static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16
  static final int MAXIMUM_CAPACITY = 1 << 30;
  static final int DEFAULT_MAX_LOOP = 1 << 4;
  static final int NBITS = 32;

  private int bucketSize;
  private int modMask;
  private int maxLoopNum;

  // table data structure
  //private int buckets [];
  long bucketPtr;
  private long keys [];
  private String values [];

  private int size = 0;

  public CukcooHashTable() {
    initBuckets(DEFAULT_INITIAL_CAPACITY);
  }

  public CukcooHashTable(int size) {
    Preconditions.checkArgument(size > 0, "Initial size cannot be more than one.");
    int findSize = findNearestPowerOfTwo(size);
    initBuckets(findSize);
  }

  public int bucketSize() {
    return bucketSize;
  }

  public int size() {
    return size;
  }

  public float load() {
    return (float)size / bucketSize;
  }

  private int findNearestPowerOfTwo(int size) {
    double y = Math.floor(Math.log(size) / Math.log(2));
    return (int)Math.pow(2, y + 1);
  }

  private void initBuckets(int bucketSize) {
    this.bucketSize = bucketSize;
    this.modMask = (bucketSize - 1);
    this.maxLoopNum = 1 << 6;

    //buckets = new int[bucketSize];
    bucketPtr = UnsafeUtil.allocVector(TajoDataTypes.Type.INT4, bucketSize);
    UnsafeUtil.unsafe.setMemory(bucketPtr, SizeOf.SIZE_OF_INT * bucketSize, (byte) 0);
    keys = new long[bucketSize + 1];
    values = new String[bucketSize + 1];

    size = 0;
  }

  public boolean insert(long hash, String value) {

    if (lookup(hash) != null) {
      return false;
    }

    Pair<Long, String> kickedOrInserted = insertEntry(hash, value);
    if (kickedOrInserted != null) {
      rehash();
      insert(kickedOrInserted.getFirst(), kickedOrInserted.getSecond());
    }
    return true;
  }

  public Pair<Long, String> insertEntry(long hash, String value) {
    int loopCount = 0;

    long kickedHash = -1;
    String kickedValue;

    long currentHash = hash;
    String currentValue = value;

    int index = (int) (hash & modMask);
    //&& kickedHash != hash
    while(loopCount < maxLoopNum) {

      kickedHash = keys[index + 1];
      kickedValue = values[index + 1];

      if (UnsafeUtil.getInt(bucketPtr, index) == 0) {
        UnsafeUtil.putInt(bucketPtr, index, index + 1);
        keys[index + 1] = currentHash;
        values[index + 1] = currentValue;
        size++;
        return null;
      }

      UnsafeUtil.putInt(bucketPtr, index, index + 1);
      keys[index + 1] = currentHash;
      values[index + 1] = currentValue;

      currentHash = kickedHash;
      currentValue = kickedValue;

      if (index == (int) (currentHash & modMask)) {
        index = (int) (currentHash >> NBITS & modMask);
      } else {
        index = (int) (currentHash & modMask);
      }

      ++loopCount;
    }

    return new Pair<Long, String>(currentHash, currentValue);
  }

  private static Log LOG = LogFactory.getLog(CukcooHashTable.class);

  public void rehash() {
    System.out.println("rehash load factor: " + load());
    int oldBucketSize = bucketSize;
    long oldBucketPtr = bucketPtr;
    long [] oldHashes = keys;
    String [] oldValues = values;

    int newBucketSize = this.bucketSize *  2;
    initBuckets(newBucketSize);

    for (int i = 0; i < oldBucketSize; i++) {
      int oldBucketIdx = UnsafeUtil.getInt(oldBucketPtr, i);
      if (oldBucketIdx != 0) {
        int idx = oldBucketIdx;
        long hash = oldHashes[idx];
        String value = oldValues[idx];
        insert(hash, value);
      }
    }

    UnsafeUtil.free(oldBucketPtr);
  }

  public String lookup(long hashKey) {
    // find the possible locations
    int buckId1 = (int) (hashKey & modMask); // use different parts of the hash number
    int buckId2 = (int) (hashKey >> NBITS & modMask);

    int idx1 = UnsafeUtil.getInt(bucketPtr, buckId1); // 0 for empty buckets,
    int idx2 = UnsafeUtil.getInt(bucketPtr, buckId2); // 1+ for non empty

    // check which one matches
    int mask1 = -(hashKey == keys[idx1] ? 1 : 0); // 0xFF..FF for a match,
    int mask2 = -(hashKey == keys[idx2] ? 1 : 0); // 0 otherwise
    int group_id = mask1 & idx1 | mask2 & idx2; // at most 1 matches

    return values[group_id];
  }
}
