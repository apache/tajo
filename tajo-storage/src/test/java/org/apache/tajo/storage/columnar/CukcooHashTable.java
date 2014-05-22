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
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.Pair;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

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

  LongKeyValueReaderWriter readerWriter;

  public CukcooHashTable() {
    this(DEFAULT_INITIAL_CAPACITY);
  }

  public CukcooHashTable(int size) {
    Preconditions.checkArgument(size > 0, "Initial size cannot be more than one.");
    readerWriter = new LongKeyValueReaderWriter();
    int findSize = findNearestPowerOfTwo(size);
    initBuckets(findSize);
  }

  public static class Slice {
    long startPtr;
    int length;

    public Slice(ByteBuffer bb) {
      DirectBuffer df = (DirectBuffer) bb;
      startPtr = df.address();
    }

    public Slice(long startPos, int length) {
      this.startPtr = startPos;
      this.length = length;
    }

    public long getLong(int offset) {
      return UnsafeUtil.unsafe.getLong(startPtr + offset);
    }
  }

  public static interface BucketReaderWriter<P> {
    public void write(long bucketPtr, P payload);
    public Slice getKey(long bucketPtr);
    public boolean equalKeys(Slice key1, Slice key2);
    public long hashKey(Slice key);
    public long hashKey(P payload);
  }

  public static class LongKeyValueReaderWriter implements BucketReaderWriter<Pair<Long, Long>> {
    public HashFunction h = Hashing.murmur3_128(37);

    @Override
    public void write(long bucketPtr, Pair<Long, Long> payload) {
      UnsafeUtil.unsafe.putLong(bucketPtr, payload.getFirst());
      bucketPtr += SizeOf.SIZE_OF_LONG;
      UnsafeUtil.unsafe.putLong(bucketPtr, payload.getSecond());
    }

    public void write(long bucketPtr, Slice payload) {
      UnsafeUtil.unsafe.copyMemory(null, payload.startPtr, null, bucketPtr, SizeOf.SIZE_OF_LONG * 2);
    }

    @Override
    public Slice getKey(long bucketPtr) {
      return new Slice(bucketPtr, SizeOf.SIZE_OF_LONG);
    }

    public Slice getBucket(long bucketPtr) {
      return new Slice(bucketPtr, SizeOf.SIZE_OF_LONG);
    }

    @Override
    public boolean equalKeys(Slice key1, Slice key2) {
      return key1.getLong(0) == key2.getLong(0);
    }

    @Override
    public long hashKey(Slice key) {
      return h.hashLong(key.getLong(0)).asLong();
    }

    @Override
    public long hashKey(Pair<Long, Long> payload) {
      return h.hashLong(payload.getFirst()).asLong();
    }

    public long hashKey(long val) {
      return h.hashLong(val).asLong();
    }
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

  public boolean insert(Pair<Long, Long> row) {

    if (contains(row.getFirst())) {
      return false;
    }

    Pair<Long, Long> kickedOrInserted = insertEntry(row);
    if (kickedOrInserted != null) {
      rehash();
      insert(kickedOrInserted);
    }
    return true;
  }

  public Pair<Long, Long> insertEntry(Pair<Long, Long> row) {
    int loopCount = 0;

    Pair<Long, Long> current = row;
    ByteBuffer bb = ByteBuffer.allocateDirect(16);
    bb.putLong(row.getFirst());
    bb.putLong(row.getSecond());
    Slice currentSlice = new Slice(bb);

    int bucketId = (int) (readerWriter.hashKey(current) & modMask);
    //&& kickedHash != hash
    while(loopCount < maxLoopNum) {

      long kickedKey = UnsafeUtil.unsafe.getLong(getKeyPtr(bucketPtr, bucketId + 1));
      long kickedValue = UnsafeUtil.unsafe.getLong(getValuePtr(bucketPtr, bucketId + 1));
      Slice kickedPayload = readerWriter.getBucket(getKeyPtr(bucketPtr, bucketId + 1));
      Pair<Long, Long> kicked = new Pair<Long, Long>(kickedKey, kickedValue);

      long keyPtr = getKeyPtr(bucketPtr, bucketId + 1);
      if (kickedPayload.getLong(0) == 0) {
        readerWriter.write(keyPtr, current);
        size++;
        return null;
      }

      readerWriter.write(keyPtr, current);
      current = kicked;

      int bucketIdFromHash1 = (int) (readerWriter.hashKey(current) & modMask);
      if (bucketId == bucketIdFromHash1) { // switch bucketId via different function
        bucketId = (int) (readerWriter.hashKey(current) >> NBITS & modMask);
      } else {
        bucketId = bucketIdFromHash1;
      }

      ++loopCount;
    }

    return current;
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
        insert(new Pair<Long, Long>(key, value));
      }
    }

    UnsafeUtil.free(oldKeysPtr);
  }

  public boolean contains(long key) {
    // find the possible locations
    int buckId1 = (int) (key & modMask) + 1; // use different parts of the hash number
    int buckId2 = (int) (key >> NBITS & modMask) + 1;

    // check which one matches
    int mask1 = -(key == UnsafeUtil.unsafe.getLong(getKeyPtr(bucketPtr, buckId1)) ? 1 : 0);
    int mask2 = -(key == UnsafeUtil.unsafe.getLong(getKeyPtr(bucketPtr, buckId2)) ? 1 : 0);

    return (mask1 | mask2) != 0;
  }

  public Long lookup(Pair<Long, Long> data) {
    long hashKey = readerWriter.hashKey(data);

    // find the possible locations
    int buckId1 = (int) (hashKey & modMask) + 1; // use different parts of the hash number
    int buckId2 = (int) (hashKey >> NBITS & modMask) + 1;

    // check which one matches
    int mask1 = -(data.getSecond() == UnsafeUtil.unsafe.getLong(getKeyPtr(bucketPtr, buckId1)) ? 1 : 0); // 0xFF..FF for a match,
    int mask2 = -(data.getSecond() == UnsafeUtil.unsafe.getLong(getKeyPtr(bucketPtr, buckId2)) ? 1 : 0); // 0 otherwise
    int group_id = mask1 & buckId1 | mask2 & buckId2; // at most 1 matches

    return group_id == 0 ? null : UnsafeUtil.unsafe.getLong(getValuePtr(bucketPtr, group_id));
  }
}
