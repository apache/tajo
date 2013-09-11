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

package org.apache.tajo.util;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestBitArrayTest {

  @Test
  public void test() {
    int numBit = 90;
    BitArray bitArray = new BitArray(numBit);
    assertEquals(numBit, bitArray.bitsLength());

    for (int i = 0; i < numBit; i++) {
      assertFalse(bitArray.get(i));
      bitArray.set(i);
      assertTrue(bitArray.get(i));
    }
  }

  @Test
  public void testFromByteBuffer() {
    int numBit = 80;
    BitArray bitArray = new BitArray(numBit);
    assertEquals(numBit, bitArray.bitsLength());

    for (int i = 0; i < numBit; i++) {
      if((i % 2) == 0){
        bitArray.set(i);
      }
    }

    ByteBuffer buffer = ByteBuffer.allocate(bitArray.bytesLength());
    buffer.put(bitArray.toArray());
    for (int i = 0; i < numBit; i++) {
      if((i % 2) == 0){
        assertTrue(bitArray.get(i));
      } else {
        assertFalse(bitArray.get(i));
      }
    }

    buffer.rewind();
    bitArray.fromByteBuffer(buffer);
    for (int i = 0; i < numBit; i++) {
      if((i % 2) == 0){
        assertTrue(bitArray.get(i));
      } else {
        assertFalse(bitArray.get(i));
      }
    }
  }
}
