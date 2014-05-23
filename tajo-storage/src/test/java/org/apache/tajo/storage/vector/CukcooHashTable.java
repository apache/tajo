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

package org.apache.tajo.storage.vector;

import com.google.common.base.Preconditions;
import org.apache.tajo.util.FileUtil;

public class CukcooHashTable<K, V, P> {
  static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16
  static final int MAXIMUM_CAPACITY = 1 << 30;
  static final int DEFAULT_MAX_LOOP = 1 << 4;
  static final int NBITS = 32;

  private int bucketNum;
  private int modMask;
  private int maxLoopNum;

  private final long perBucketSize;

  // table data structure
  private long bucketPtr;

  private int size = 0;

  BucketReaderWriter<K,V,P> readerWriter;

  UnsafeBuf kickedBucket[] = new UnsafeBuf[2];
  UnsafeBuf currentBuf;
  UnsafeBuf rehash;

  public CukcooHashTable(BucketReaderWriter bucketHandler) {
    this(DEFAULT_INITIAL_CAPACITY, bucketHandler);
  }

  public CukcooHashTable(int size, BucketReaderWriter bucketHandler) {
    Preconditions.checkArgument(size > 0, "Initial size cannot be more than one.");
    readerWriter = bucketHandler;
    perBucketSize = readerWriter.getPayloadSize();
    int findSize = findNearestPowerOfTwo(size);
    initBuckets(findSize);

    currentBuf = readerWriter.newBucketBuffer();

    kickedBucket[0] = readerWriter.newBucketBuffer();
    kickedBucket[1] = readerWriter.newBucketBuffer();

    rehash = readerWriter.newBucketBuffer();
  }

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

  private long getBucketAddr(long basePtr, long bucketId) {
    return basePtr + (perBucketSize * bucketId);
  }

  private void initBuckets(int bucketNum) {
    this.bucketNum = bucketNum;
    this.modMask = (bucketNum - 1);
    this.maxLoopNum = 1 << 6;

    long totalMemory = (perBucketSize * (bucketNum + 1));
    bucketPtr = UnsafeUtil.alloc(totalMemory);
    UnsafeUtil.unsafe.setMemory(bucketPtr, totalMemory, (byte) 0);
    size = 0;

    System.out.println("consumed memory:" + FileUtil.humanReadableByteCount(totalMemory, true));
  }

  private int computeOneBucketSize() {
    return SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_LONG;
  }

  public boolean insert(UnsafeBuf row) {

    if (contains(row)) {
      return false;
    }

    UnsafeBuf kickedOrInserted = insertEntry(row);
    if (kickedOrInserted != null) {
      rehash();
      insert(kickedOrInserted);
    }
    return true;
  }

  int switchIdx = 0;

  public UnsafeBuf insertEntry(UnsafeBuf row) {
    int loopCount = 0;

    row.copyTo(currentBuf);

    int bucketId = (int) (readerWriter.hashFunc(currentBuf) & modMask);
    while(loopCount < maxLoopNum) {
      long keyPtr = getBucketAddr(bucketPtr, bucketId + 1);
      readerWriter.getBucket(keyPtr, kickedBucket[switchIdx]);
      if (kickedBucket[switchIdx].getLong(0) == 0) {
        readerWriter.write(keyPtr, currentBuf);
        size++;
        return null;
      }

      readerWriter.write(keyPtr, currentBuf);
      currentBuf = kickedBucket[switchIdx];

      int bucketIdFromHash1 = (int) (readerWriter.hashFunc(currentBuf) & modMask);
      if (bucketId == bucketIdFromHash1) { // switch bucketId via different function
        bucketId = (int) (readerWriter.hashFunc(currentBuf) >> NBITS & modMask);
      } else {
        bucketId = bucketIdFromHash1;
      }

      switchIdx ^= 1;
      ++loopCount;
    }

    return currentBuf.copyOf();
  }

  public void rehash() {
    int oldBucketSize = bucketNum;
    long oldKeysPtr = bucketPtr;
    int newBucketSize = this.bucketNum *  2;
    System.out.println("rehash load factor: " + load());

    initBuckets(newBucketSize);

    for (int i = 0; i < oldBucketSize; i++) {
      int actualIdx = i + 1;

      readerWriter.getBucket(getBucketAddr(oldKeysPtr, actualIdx), rehash);
      if (rehash.getLong(0) != 0) {
        insert(rehash);
      }
    }

    UnsafeUtil.free(oldKeysPtr);
  }

  public boolean contains(UnsafeBuf unsafeBuf) {
    long hashKey = readerWriter.hashFunc(unsafeBuf);

    // find the possible locations
    int buckId1 = (int) (hashKey & modMask) + 1; // use different parts of the hash number
    int buckId2 = (int) (hashKey >> NBITS & modMask) + 1;

    // check which one matches
    int mask1 = -(unsafeBuf.getLong(0) == UnsafeUtil.unsafe.getLong(getBucketAddr(bucketPtr, buckId1)) ? 1 : 0);
    int mask2 = -(unsafeBuf.getLong(0) == UnsafeUtil.unsafe.getLong(getBucketAddr(bucketPtr, buckId2)) ? 1 : 0);

    return (mask1 | mask2) != 0;
  }

  public V lookup(K key) {
    long hashKey = readerWriter.hashFunc(key);

    // find the possible locations
    int buckId1 = (int) (hashKey & modMask) + 1; // use different parts of the hash number
    int buckId2 = (int) (hashKey >> NBITS & modMask) + 1;

    // check which one matches
    int mask1 = -(key.equals(readerWriter.getKey(getBucketAddr(bucketPtr, buckId1))) ? 1 : 0); // 0xFF..FF for a match,
    int mask2 = -(key.equals(readerWriter.getKey(getBucketAddr(bucketPtr, buckId2))) ? 1 : 0); // 0 otherwise
    int group_id = mask1 & buckId1 | mask2 & buckId2; // at most 1 matches

    return group_id == 0 ? null : readerWriter.getValue(getBucketAddr(bucketPtr, group_id));
  }

  public V lookup(UnsafeBuf key) {
    long hashKey = readerWriter.hashFunc(key);

    // find the possible locations
    int buckId1 = (int) (hashKey & modMask) + 1; // use different parts of the hash number
    int buckId2 = (int) (hashKey >> NBITS & modMask) + 1;

    // check which one matches
    int mask1 = -(key.getLong(0) == UnsafeUtil.unsafe.getLong(getBucketAddr(bucketPtr, buckId1)) ? 1 : 0); // 0xFF..FF for a match,
    int mask2 = -(key.getLong(0) == UnsafeUtil.unsafe.getLong(getBucketAddr(bucketPtr, buckId2)) ? 1 : 0); // 0 otherwise
    int group_id = mask1 & buckId1 | mask2 & buckId2; // at most 1 matches

    return group_id == 0 ? null : readerWriter.getValue(getBucketAddr(bucketPtr, group_id));
  }
}
