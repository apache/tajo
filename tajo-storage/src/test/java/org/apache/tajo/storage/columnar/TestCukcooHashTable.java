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
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.tajo.storage.columnar.map.VecFuncMulMul3LongCol;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestCukcooHashTable {

  public static void main(String [] args) {
    String [] strs = {
        "hyunsik",
        "tajo",
        "abc",
        "def",
        "gef",
        "abd",
        "nml",
        "apache",
        "daum",
        "www",
        "eclipse",
        "qwejklqwe",
        "asjdlqkwe",
        "anm,23"
    };

    CukcooHashTable hashTable = new CukcooHashTable();
    System.out.println(hashTable.bucketSize());

    HashFunction hf = Hashing.murmur3_128(64);

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < (1 << 22); i++) {
      long key = hf.hashLong(i).asLong();

      Long found = hashTable.lookup(key);

      assertTrue(found == null);

      hashTable.insert(key, i);

      assertTrue(i == (hashTable.lookup(key)));

      if (hashTable.size() != i + 1) {
        System.out.println("Error point!");
      }
    }
    long writeEnd = System.currentTimeMillis();

    System.out.println((writeEnd - writeStart) + " msc write time");
    System.out.println(">> Size: " + hashTable.size());

    long start = System.currentTimeMillis();
    for (int i = 0; i < (1 << 22); i++) {
      long value1 = i;
      long key1 = hf.hashLong(value1).asLong();
      assertTrue(value1 == hashTable.lookup(key1));
    }
    long end = System.currentTimeMillis();
    System.out.println((end - start) + " msc read time");
  }

  @Test
  public void testHashMap() {
    HashFunction hf = Hashing.murmur3_128(37);
    Map<Long, Long> map = Maps.newHashMap();

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < (1 << 22); i++) {
      long key = hf.hashLong(i).asLong();
      Long found = map.get(key);

      assertTrue(found == null);

      map.put(key, new Long(i));

      assertTrue(i == (map.get(key)));

       if (map.size() != i + 1) {
        System.out.println("Error point!");
      }
    }
    long writeEnd = System.currentTimeMillis();

    System.out.println((writeEnd - writeStart) + " msc write time");
    System.out.println(">> Size: " + map.size());


    long start = System.currentTimeMillis();
    for (int i = 0; i < (1 << 22); i++) {
      long key = hf.hashLong(i).asLong();
      assertTrue(i == map.get(key));
    }
    long end = System.currentTimeMillis();
    System.out.println((end - start) + " msc read time");
  }
}
