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

import com.google.common.collect.Maps;
import org.apache.tajo.util.Pair;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCukcooHashTable {

  @Test
  public void testCukcoo() {
    LongKeyValueReaderWriter bucketHandler = new LongKeyValueReaderWriter();
    CukcooHashTable hashTable = new CukcooHashTable(bucketHandler);
    System.out.println(hashTable.bucketSize());

    ByteBuffer bb = ByteBuffer.allocateDirect(16);
    bb.order(ByteOrder.nativeOrder());
    UnsafeBuf unsafeBuf = new UnsafeBuf(bb);
    long writeStart = System.currentTimeMillis();
    for (int i = 1; i < (1 << 25); i++) {

      Long v = new Long(i);
      unsafeBuf.putLong(0, v);
      unsafeBuf.putLong(8, v);

      Pair p = new Pair<Long, Long>(v, v);
      Long found = hashTable.lookup(unsafeBuf);

      assertTrue(found == null);

      hashTable.insert(unsafeBuf);

      found = hashTable.lookup(unsafeBuf);
      assertEquals(v, found);

      if (hashTable.size() != i) {
        System.out.println("Error point!");
      }
    }
    long writeEnd = System.currentTimeMillis();

    System.out.println((writeEnd - writeStart) + " msc write time");
    System.out.println(">> Size: " + hashTable.size());

    long start = System.currentTimeMillis();
    for (int i = 1; i < (1 << 25); i++) {
      Long val = new Long(i);
      unsafeBuf.putLong(0, val);
      assertEquals(val, hashTable.lookup(unsafeBuf));
    }
    long end = System.currentTimeMillis();
    System.out.println((end - start) + " msc sequential read time");

    long startRandom = System.currentTimeMillis();
    Random rnd = new Random(System.currentTimeMillis());
    for (int i = 1; i < (1 << 25); i++) {
      Long val = new Long(rnd.nextInt(1 << 23));
      unsafeBuf.putLong(0, val);
      assertEquals(val, hashTable.lookup(unsafeBuf));
    }
    long endRandom = System.currentTimeMillis();
    System.out.println((endRandom - startRandom) + " msc random read time");
  }

  @Test
  public void testHashMap() {
    Map<Long, Long> map = Maps.newHashMap();

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < (1 << 23); i++) {
      Long key = new Long(i);

      Long found = map.get(key);

      assertTrue(found == null);

      map.put(key, key);

      assertEquals(key, map.get(key));

       if (map.size() != i + 1) {
        System.out.println("Error point!");
      }
    }
    long writeEnd = System.currentTimeMillis();

    System.out.println((writeEnd - writeStart) + " msc write time");
    System.out.println(">> Size: " + map.size());


    long start = System.currentTimeMillis();
    for (int i = 0; i < (1 << 23); i++) {
      Long key = new Long(i);
      assertEquals(key, map.get(key));
    }
    long end = System.currentTimeMillis();
    System.out.println((end - start) + " msc sequential read time");

    long startRandom = System.currentTimeMillis();
    Random rnd = new Random(System.currentTimeMillis());
    for (int i = 1; i < (1 << 23); i++) {
      Long val = new Long(rnd.nextInt(1 << 23));
      assertEquals(val, map.get(val));
    }
    long endRandom = System.currentTimeMillis();
    System.out.println((endRandom - startRandom) + " msc random read time");
  }

  public static class LongKeyValueReaderWriter implements CukcooHashTable.BucketReaderWriter<Pair<Long, Long>> {

    @Override
    public void write(long bucketPtr, Pair<Long, Long> payload) {
      UnsafeUtil.unsafe.putLong(bucketPtr, payload.getFirst());
      bucketPtr += SizeOf.SIZE_OF_LONG;
      UnsafeUtil.unsafe.putLong(bucketPtr, payload.getSecond());
    }

    @Override
    public UnsafeBuf newBucketBuffer() {
      ByteBuffer byteBuffer = ByteBuffer.allocateDirect(16);
      byteBuffer.order(ByteOrder.nativeOrder());
      return new UnsafeBuf(byteBuffer);
    }

    public void write(long bucketPtr, UnsafeBuf payload) {
      UnsafeUtil.unsafe.copyMemory(null, payload.address, null, bucketPtr, SizeOf.SIZE_OF_LONG * 2);
    }

    @Override
    public UnsafeBuf getKey(long bucketPtr) {
      return new UnsafeBuf(bucketPtr, SizeOf.SIZE_OF_LONG);
    }

    @Override
    public void getBucket(long bucketPtr, UnsafeBuf target) {
      UnsafeUtil.unsafe.copyMemory(null, bucketPtr, null, target.address, 16);
    }

    @Override
    public boolean equalKeys(UnsafeBuf key1, UnsafeBuf key2) {
      return key1.getLong(0) == key2.getLong(0);
    }

    @Override
    public long hashFunc1(UnsafeBuf key) {
      return key.getLong(0);
    }

    @Override
    public long hashKey(Pair<Long, Long> payload) {
      return payload.getFirst();
    }

    public long hashKey(long val) {
      return val;
    }
  }
}
