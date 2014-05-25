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
  public long bucketPtr;

  private int size = 0;

  private final BucketHandler<K,V> bucketHandler;

  private final UnsafeBuf kickingBucket[] = new UnsafeBuf[2];
  private UnsafeBuf currentBuf;
  private final UnsafeBuf rehash;

  public CukcooHashTable(BucketHandler bucketHandler) {
    this(DEFAULT_INITIAL_CAPACITY, bucketHandler);
  }

  public CukcooHashTable(int size, BucketHandler bucketHandler) {
    Preconditions.checkArgument(size > 0, "Initial size cannot be more than one.");
    this.bucketHandler = bucketHandler;
    perBucketSize = this.bucketHandler.getBucketSize();
    int findSize = findNearestPowerOfTwo(size);
    initBuckets(findSize);

    currentBuf = this.bucketHandler.createBucketBuffer();

    kickingBucket[0] = this.bucketHandler.createBucketBuffer();
    kickingBucket[1] = this.bucketHandler.createBucketBuffer();

    rehash = this.bucketHandler.createBucketBuffer();
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

  public boolean insert(UnsafeBuf payload) {

    if (contains(payload)) {
      return false;
    }

    UnsafeBuf kickedOrInserted = insertEntry(payload);
    if (kickedOrInserted != null) {
      rehash();
      insert(kickedOrInserted);
    }
    return true;
  }

  public boolean insert(long hash, UnsafeBuf payload) {
    if (contains(payload)) {
      return false;
    }

    UnsafeBuf kickedOrInserted = insertEntryInternal(hash, payload);
    if (kickedOrInserted != null) {
      rehash();
      insert(kickedOrInserted);
    }
    return true;
  }

  int switchBetweenZeroAndOne = 0;

  private UnsafeBuf insertEntry(UnsafeBuf payload) {
    long hash = bucketHandler.hashFunc(payload);
    return insertEntryInternal(hash, payload);
  }

  private UnsafeBuf insertEntryInternal(long hash, UnsafeBuf payload) {
    int loopCount = 0;

    int bucketId = (int) (hash & modMask);
    payload.copyTo(this.currentBuf);
    UnsafeBuf currentBucket = this.currentBuf;

    while(loopCount < maxLoopNum) {
      long keyPtr = getBucketAddr(bucketPtr, bucketId + 1);
      bucketHandler.getBucket(keyPtr, kickingBucket[switchBetweenZeroAndOne]);
      if (bucketHandler.isEmptyBucket(kickingBucket[switchBetweenZeroAndOne].address)) {
        bucketHandler.write(keyPtr, currentBucket);
        size++;
        return null;
      }

      bucketHandler.write(keyPtr, currentBucket);
      currentBucket = kickingBucket[switchBetweenZeroAndOne];

      int bucketIdFromHash1 = (int) (bucketHandler.hashFunc(currentBucket) & modMask);
      if (bucketId == bucketIdFromHash1) { // switch bucketId via different function
        bucketId = (int) (bucketHandler.hashFunc(currentBucket) >> NBITS & modMask);
      } else {
        bucketId = bucketIdFromHash1;
      }

      switchBetweenZeroAndOne ^= 1;
      ++loopCount;
    }

    return currentBucket.copyOf();
  }

  public void rehash() {
    int oldBucketSize = bucketNum;
    long oldKeysPtr = bucketPtr;
    int newBucketSize = this.bucketNum *  2;
    System.out.println("rehash load factor: " + load());

    initBuckets(newBucketSize);

    for (int i = 0; i < oldBucketSize; i++) {
      int actualIdx = i + 1;

      bucketHandler.getBucket(getBucketAddr(oldKeysPtr, actualIdx), rehash);
      if (!bucketHandler.isEmptyBucket(rehash.address)) {
        insert(rehash);
      }
    }

    UnsafeUtil.free(oldKeysPtr);
  }

  public boolean contains(UnsafeBuf unsafeBuf) {
    long hashKey = bucketHandler.hashFunc(unsafeBuf);

    // find the possible locations
    int buckId1 = (int) (hashKey & modMask) + 1; // use different parts of the hash number
    int buckId2 = (int) (hashKey >> NBITS & modMask) + 1;

    // check which one matches
    int mask1 = -(bucketHandler.equalKeys(unsafeBuf, getBucketAddr(bucketPtr, buckId1)) ? 1 : 0);
    int mask2 = -(bucketHandler.equalKeys(unsafeBuf, getBucketAddr(bucketPtr, buckId2)) ? 1 : 0);

    return (mask1 | mask2) != 0;
  }

  public V getValue(K key) {
    long hashKey = bucketHandler.hashFunc(key);

    // find the possible locations
    int buckId1 = (int) (hashKey & modMask) + 1; // use different parts of the hash number
    int buckId2 = (int) (hashKey >> NBITS & modMask) + 1;

    // check which one matches
    int mask1 = -(key.equals(bucketHandler.getKey(getBucketAddr(bucketPtr, buckId1))) ? 1 : 0); // 0xFF..FF for a match,
    int mask2 = -(key.equals(bucketHandler.getKey(getBucketAddr(bucketPtr, buckId2))) ? 1 : 0); // 0 otherwise
    int group_id = mask1 & buckId1 | mask2 & buckId2; // at most 1 matches

    return group_id == 0 ? null : bucketHandler.getValue(getBucketAddr(bucketPtr, group_id));
  }

  private int getGroupId(UnsafeBuf key) {
    long hashKey = bucketHandler.hashFunc(key);

    // find the possible locations
    int buckId1 = (int) (hashKey & modMask) + 1; // use different parts of the hash number
    int buckId2 = (int) (hashKey >> NBITS & modMask) + 1;

    // check which one matches
    int mask1 = -(bucketHandler.equalKeys(key, getBucketAddr(bucketPtr, buckId1)) ? 1 : 0); // 0xFF..FF for a match,
    int mask2 = -(bucketHandler.equalKeys(key, getBucketAddr(bucketPtr, buckId2)) ? 1 : 0); // 0 otherwise
    return mask1 & buckId1 | mask2 & buckId2; // at most 1 matches
  }

  public V getValue(UnsafeBuf key) {
    int groupId = getGroupId(key);
    return groupId == 0 ? null : bucketHandler.getValue(getBucketAddr(bucketPtr, groupId));
  }

  public UnsafeBuf getPayload(UnsafeBuf key, UnsafeBuf buffer) {
    int groupId = getGroupId(key);
    return groupId == 0 ? null : bucketHandler.getBucket(getBucketAddr(bucketPtr, groupId), buffer);
  }

  public void findGroupIds(int vecNum, /* compacted */ int[] groupIds, long hashVec, long valueVec, int[] selVec) {
    long hashOffset = 0;
    long valueOffset = 0;
    for (int i = 0; i < vecNum; i++) {
      hashOffset = selVec[i] * SizeOf.SIZE_OF_LONG;
      valueOffset = selVec[i] * 2;

      long hash = UnsafeUtil.unsafe.getLong(hashVec + hashOffset);

      // find the possible locations
      int buckId1 = (int) (hash & modMask) + 1; // use different parts of the hash number
      int buckId2 = (int) (hash >> NBITS & modMask) + 1;

      // check which one matches
      int mask1 = -(bucketHandler.equalKeys(valueVec + valueOffset, getBucketAddr(bucketPtr, buckId1)) ? 1 : 0); // 0xFF..FF for a match,
      int mask2 = -(bucketHandler.equalKeys(valueVec + valueOffset, getBucketAddr(bucketPtr, buckId2)) ? 1 : 0); // 0 otherwise
      groupIds[i] = mask1 & buckId1 | mask2 & buckId2; // at most 1 matches
    }
  }

  public void insertMissedGroups(int vecNum, /* compacted */ boolean [] inserted, int[] groupIds, long hashVec, long keyVector, long [] valueVecs, int[] selVec) {
    int hashVecOffset;
    int keyVecOffset;
    int payloadOffset;
    UnsafeBuf payload = bucketHandler.createBucketBuffer();
    for (int i = 0; i < vecNum; i++) {
      if (groupIds[i] == 0) {
        hashVecOffset = selVec[i] * SizeOf.SIZE_OF_LONG;
        keyVecOffset = selVec[i] * 2;
        payloadOffset = 0;

        // reuse key pivot
        payload.putBytes(0, keyVector + keyVecOffset, 2);

        // DSM -> NSM
        payloadOffset += 2;
        payload.putBytes(payloadOffset, valueVecs[0], 8); // sum(l_quantity)
        payloadOffset+= SizeOf.SIZE_OF_DOUBLE;
        payload.putBytes(payloadOffset, valueVecs[1], 8); // sum(l_extendedprice)
        payloadOffset+= SizeOf.SIZE_OF_DOUBLE;
        payload.putBytes(payloadOffset, valueVecs[2], 8); // sum(l_extendedprice*(1-l_discount))
        payloadOffset+= SizeOf.SIZE_OF_DOUBLE;
        payload.putBytes(payloadOffset, valueVecs[3], 8); // sum(l_extendedprice*(1-l_discount)*(1+l_tax))
        payloadOffset+= SizeOf.SIZE_OF_DOUBLE;

        payload.putBytes(payloadOffset, valueVecs[0], 8); // avg(l_quantity) : sum
        payloadOffset+= SizeOf.SIZE_OF_DOUBLE;
        payload.putLong(payloadOffset, 0);                // avg(l_quantity) : count
        payloadOffset+= SizeOf.SIZE_OF_LONG;

        payload.putBytes(payloadOffset, valueVecs[1], 8); // avg(l_extendedprice) : sum
        payloadOffset+= SizeOf.SIZE_OF_DOUBLE;
        payload.putLong(payloadOffset, 0);                // avg(l_extendedprice) : count
        payloadOffset+= SizeOf.SIZE_OF_LONG;


        payload.putBytes(payloadOffset, valueVecs[5], 8); // avg(l_discount) : sum
        payloadOffset+= SizeOf.SIZE_OF_DOUBLE;
        payload.putLong(payloadOffset, 0);                // avg(l_discount) : count
        payloadOffset+= SizeOf.SIZE_OF_LONG;

        payload.putLong(payloadOffset, 0);                // count(*)

        long hash = UnsafeUtil.unsafe.getLong(hashVec + hashVecOffset);
        insert(hash, payload);
      }
    }
  }

  public void computeAggregate(int vecNum, /* compacted */ boolean [] inserted, int[] groupIds, long hashVec, long keyVector, long [] valueVecs, int[] selVec) {
    int hashVecOffset;
    int payloadOffset;
    long valueOffset = 0;
    UnsafeBuf bucket = bucketHandler.createBucketBuffer();
    for (int i = 0; i < vecNum; i++) {
      if (groupIds[i] != 0) {
        hashVecOffset = selVec[i] * SizeOf.SIZE_OF_LONG;
        payloadOffset = 0;
        valueOffset = selVec[i] * SizeOf.SIZE_OF_LONG;

        bucketHandler.getBucket(getBucketAddr(bucketPtr, groupIds[i]), bucket);

        // skip keys
        payloadOffset += 2;


        // compute

        // 0: sum (l_quantity) : double
        double lhs = UnsafeUtil.unsafe.getDouble(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset);
        double rhs = UnsafeUtil.unsafe.getDouble(valueVecs[0] + valueOffset);
        double res = lhs + rhs;
        UnsafeUtil.unsafe.putDouble(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset, res);

        payloadOffset += SizeOf.SIZE_OF_DOUBLE;

        // 1: sum(l_extendedprice) : double
        lhs = UnsafeUtil.unsafe.getDouble(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset);
        rhs = UnsafeUtil.unsafe.getDouble(valueVecs[1] + valueOffset);
        res = lhs + rhs;
        UnsafeUtil.unsafe.putDouble(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset, res);


        payloadOffset += SizeOf.SIZE_OF_DOUBLE;

        // 2: sum(l_extendedprice*(1-l_discount)) : double
        lhs = UnsafeUtil.unsafe.getDouble(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset);
        rhs = UnsafeUtil.unsafe.getDouble(valueVecs[2] + valueOffset);
        res = lhs + rhs;
        UnsafeUtil.unsafe.putDouble(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset, res);

        payloadOffset += SizeOf.SIZE_OF_DOUBLE;

        // 3: sum(l_extendedprice*(1-l_discount)*(1+l_tax) : double
        lhs = UnsafeUtil.unsafe.getDouble(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset);
        rhs = UnsafeUtil.unsafe.getDouble(valueVecs[3] + valueOffset);
        res = lhs + rhs;
        UnsafeUtil.unsafe.putDouble(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset, res);

        payloadOffset += SizeOf.SIZE_OF_DOUBLE;

        // 4-1: avg(l_quantity) - sum : double
        lhs = UnsafeUtil.unsafe.getDouble(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset);
        rhs = UnsafeUtil.unsafe.getDouble(valueVecs[0] + valueOffset);
        res = lhs + rhs;
        UnsafeUtil.unsafe.putDouble(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset, res);
        payloadOffset += SizeOf.SIZE_OF_DOUBLE;
        // 4-2: avg(l_quantity) - count : long
        long cnt = UnsafeUtil.unsafe.getLong(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset);
        UnsafeUtil.unsafe.putLong(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset, cnt + 1);
        payloadOffset += SizeOf.SIZE_OF_LONG;

        // 5-1: avg(l_extendedprice) - sum : double
        lhs = UnsafeUtil.unsafe.getDouble(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset);
        rhs = UnsafeUtil.unsafe.getDouble(valueVecs[1] + valueOffset);
        res = lhs + rhs;
        UnsafeUtil.unsafe.putDouble(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset, res);
        payloadOffset += SizeOf.SIZE_OF_DOUBLE;
        // 5-2: avg(l_extendedprice) - count : long
        cnt = UnsafeUtil.unsafe.getLong(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset);
        UnsafeUtil.unsafe.putLong(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset, cnt + 1);
        payloadOffset += SizeOf.SIZE_OF_LONG;

        // 6-1: avg(l_discount) - sum : double
        lhs = UnsafeUtil.unsafe.getDouble(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset);
        rhs = UnsafeUtil.unsafe.getDouble(valueVecs[5] + valueOffset);
        res = lhs + rhs;
        UnsafeUtil.unsafe.putDouble(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset, res);
        payloadOffset += SizeOf.SIZE_OF_DOUBLE;
        // 6-2: avg(l_discount) - count : long
        cnt = UnsafeUtil.unsafe.getLong(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset);
        UnsafeUtil.unsafe.putLong(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset, cnt + 1);
        payloadOffset += SizeOf.SIZE_OF_LONG;

        // 7: count(*) : long
        cnt = UnsafeUtil.unsafe.getLong(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset);
        UnsafeUtil.unsafe.putLong(getBucketAddr(bucketPtr, groupIds[i]) + payloadOffset, cnt + 1);
      }
    }
  }
}
