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
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.Pair;

public class CukcooHashTable {
  static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16
  static final int MAXIMUM_CAPACITY = 1 << 30;
  static final int DEFAULT_MAX_LOOP = 1 << 4;
  static final int NBITS = 32;

  private int bucketNum;
  private int modMask;
  private int maxLoopNum;

  // table data structure
  private long bucketPtr;

  private int size = 0;

  public CukcooHashTable() {
    initBuckets(DEFAULT_INITIAL_CAPACITY);
  }

  public CukcooHashTable(int size) {
    Preconditions.checkArgument(size > 0, "Initial size cannot be more than one.");
    int findSize = findNearestPowerOfTwo(size);
    initBuckets(findSize);
  }
//
//  public static class BucketReader {
//    public void write(long bucketPtr);
//  }

  public int bucketSize() {
    return bucketNum;
  }

  public int size() {
    return size;
  }

  public float load() {
    return (float)size / bucketNum;
  }

  private int findNearestPowerOfTwo(int size) {
    double y = Math.floor(Math.log(size) / Math.log(2));
    return (int)Math.pow(2, y + 1);
  }

  private long computeBucketSize() {
    return SizeOf.SIZE_OF_LONG + SizeOf.SIZE_OF_LONG;
  }

  private long getKeyPtr(long basePtr, int bucketId) {
    return basePtr + (perBucketSize * bucketId);
  }

  private long getValuePtr(long basePtr, int bucketId) {
    return basePtr + (perBucketSize * bucketId) + SizeOf.SIZE_OF_LONG;
  }

  private final long perBucketSize = computeBucketSize();

  private void initBuckets(int bucketNum) {
    this.bucketNum = bucketNum;
    this.modMask = (bucketNum - 1);
    this.maxLoopNum = 1 << 6;

    long perBucketSize = computeBucketSize();
    long totalMemory = (perBucketSize * (bucketNum + 1));
    bucketPtr = UnsafeUtil.alloc(totalMemory);
    UnsafeUtil.unsafe.setMemory(bucketPtr, totalMemory, (byte) 0);
    size = 0;

    System.out.println("consumed memory:" + FileUtil.humanReadableByteCount(totalMemory, true));
  }

  private int computeOneBucketSize() {
    return SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_LONG;
  }

  public boolean insert(long hash, long value) {

    if (contains(hash)) {
      return false;
    }

    Pair<Long, Long> kickedOrInserted = insertEntry(hash, value);
    if (kickedOrInserted != null) {
      rehash();
      insert(kickedOrInserted.getFirst(), kickedOrInserted.getSecond());
    }
    return true;
  }

  public Pair<Long, Long> insertEntry(long hash, long value) {
    int loopCount = 0;

    long kickedHash = -1;
    Long kickedValue;

    long currentHash = hash;
    long currentValue = value;

    int index = (int) (hash & modMask);
    //&& kickedHash != hash
    while(loopCount < maxLoopNum) {

      kickedHash = UnsafeUtil.unsafe.getLong(getKeyPtr(bucketPtr, index + 1));
      kickedValue = UnsafeUtil.unsafe.getLong(getValuePtr(bucketPtr, index + 1));

      long keyPtr = getKeyPtr(bucketPtr, index + 1);
      if (UnsafeUtil.unsafe.getLong(keyPtr) == 0) {
        UnsafeUtil.unsafe.putLong(keyPtr, currentHash);
        UnsafeUtil.unsafe.putLong(getValuePtr(bucketPtr, index + 1), currentValue);
        size++;
        return null;
      }

      UnsafeUtil.unsafe.putLong(keyPtr, currentHash);
      UnsafeUtil.unsafe.putLong(getValuePtr(bucketPtr, index + 1), currentValue);

      currentHash = kickedHash;
      currentValue = kickedValue;

      if (index == (int) (currentHash & modMask)) {
        index = (int) (currentHash >> NBITS & modMask);
      } else {
        index = (int) (currentHash & modMask);
      }

      ++loopCount;
    }

    return new Pair<Long, Long>(currentHash, currentValue);
  }

  public void rehash() {
    int oldBucketSize = bucketNum;
    long oldKeysPtr = bucketPtr;
    int newBucketSize = this.bucketNum *  2;
    System.out.println("rehash load factor: " + load());

    initBuckets(newBucketSize);

    for (int i = 0; i < oldBucketSize; i++) {
      int actualIdx = i + 1;

      long key = UnsafeUtil.unsafe.getLong(getKeyPtr(oldKeysPtr, actualIdx));
      if (key != 0) {
        long value = UnsafeUtil.unsafe.getLong(getValuePtr(oldKeysPtr, actualIdx));
        insert(key, value);
      }
    }

    UnsafeUtil.free(oldKeysPtr);
  }

  public boolean contains(long hashKey) {
    // find the possible locations
    int buckId1 = (int) (hashKey & modMask) + 1; // use different parts of the hash number
    int buckId2 = (int) (hashKey >> NBITS & modMask) + 1;

    // check which one matches
    int mask1 = -(hashKey == UnsafeUtil.unsafe.getLong(getKeyPtr(bucketPtr, buckId1)) ? 1 : 0);
    int mask2 = -(hashKey == UnsafeUtil.unsafe.getLong(getKeyPtr(bucketPtr, buckId2)) ? 1 : 0);

    return (mask1 | mask2) != 0;
  }

  public Long lookup(long hashKey) {
    // find the possible locations
    int buckId1 = (int) (hashKey & modMask) + 1; // use different parts of the hash number
    int buckId2 = (int) (hashKey >> NBITS & modMask) + 1;

    // check which one matches
    int mask1 = -(hashKey == UnsafeUtil.unsafe.getLong(getKeyPtr(bucketPtr, buckId1)) ? 1 : 0); // 0xFF..FF for a match,
    int mask2 = -(hashKey == UnsafeUtil.unsafe.getLong(getKeyPtr(bucketPtr, buckId2)) ? 1 : 0); // 0 otherwise
    int group_id = mask1 & buckId1 | mask2 & buckId2; // at most 1 matches

    return group_id == 0 ? null : UnsafeUtil.unsafe.getLong(getValuePtr(bucketPtr, group_id));
  }
}
