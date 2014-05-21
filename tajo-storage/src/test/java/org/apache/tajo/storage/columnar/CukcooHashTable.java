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
  private int regionSize;
  private int modMask;
  private int maxLoopNum;

  // table data structure
  private int buckets [];
  private long keys [];
  private String values [];

  private int count = 0;

  public CukcooHashTable() {
    initBuckets(DEFAULT_INITIAL_CAPACITY);
  }

  public CukcooHashTable(int size) {
    Preconditions.checkArgument(size > 0, "Initial size cannot be more than one.");
    int findSize = findNearestPowerOfTwo(size);
    initBuckets(findSize);
  }

  public int size() {
    return count;
  }

  private int findNearestPowerOfTwo(int size) {
    return size == 0 ? 0 : 32 - Integer.numberOfLeadingZeros(size - 1);
  }

  private void initBuckets(int bucketSize) {
    this.bucketSize = bucketSize;
    this.regionSize = bucketSize / 2;
    this.modMask = (bucketSize - 1);
    this.maxLoopNum = 1 << 4;

    buckets = new int[bucketSize];
    keys = new long[bucketSize + 1];
    values = new String[bucketSize + 1];

    count = 0;
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

    int index = (int) (hash & modMask) % regionSize;
    while(loopCount < maxLoopNum && kickedHash != hash) {

      kickedHash = keys[index + 1];
      kickedValue = values[index + 1];

      if (buckets[index] == 0) {
        buckets[index] = index + 1;
        keys[index + 1] = currentHash;
        values[index + 1] = currentValue;
        count++;
        return null;
      }

      buckets[index] = index + 1;
      keys[index + 1] = currentHash;
      values[index + 1] = currentValue;

      currentHash = kickedHash;
      currentValue = kickedValue;

      if (index == (int) (currentHash & modMask) % regionSize) {
        index = (int) (currentHash >> NBITS & modMask) % regionSize + regionSize;
      } else {
        index = (int) (currentHash & modMask) % regionSize;
      }

      ++loopCount;
    }

    return new Pair<Long, String>(currentHash, currentValue);
  }

  public void rehash() {
    int [] oldBuckets = buckets;
    long [] oldHashes = keys;
    String [] oldValues = values;

    int newBucketSize = this.bucketSize *  2;
    initBuckets(newBucketSize);

    for (int i = 0; i < oldBuckets.length; i++) {
      if (oldBuckets[i] != 0) {
        int idx = oldBuckets[i];
        long hash = oldHashes[idx];
        String value = oldValues[idx];
        insert(hash, value);
      }
    }
  }

  public String lookup(long hashKey) {
    // find the possible locations
    int buckId1 = (int) (hashKey & modMask) % regionSize; // use different parts of the hash number
    int buckId2 = (int) (hashKey >> NBITS & modMask) % regionSize + regionSize;

    int idx1 = buckets[buckId1]; // 0 for empty buckets,
    int idx2 = buckets[buckId2]; // 1+ for non empty

    // check which one matches
    int mask1 = -(hashKey == keys[idx1] ? 1 : 0); // 0xFF..FF for a match,
    int mask2 = -(hashKey == keys[idx2] ? 1 : 0); // 0 otherwise
    int group_id = mask1 & idx1 | mask2 & idx2; // at most 1 matches

    return values[group_id];
  }
}
