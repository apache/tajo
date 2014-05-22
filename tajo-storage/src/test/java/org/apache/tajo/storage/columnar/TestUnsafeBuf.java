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

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by hyunsik on 5/23/14.
 */
public class TestUnsafeBuf {
  @Test
  public void testUnsafeBuf() {
    ByteBuffer bb = ByteBuffer.allocateDirect(16);
    UnsafeBuf b1 = new UnsafeBuf(bb);
    UnsafeBuf b2 = b1.copyOf();
    assertTrue(b1.address != b2.address);
  }

  @Test
  public void testEquals() {
    ByteBuffer bb = ByteBuffer.allocateDirect(16);
    UnsafeBuf b1 = new UnsafeBuf(bb);
    UnsafeBuf b2 = b1.copyOf();
    assertEquals(b1, b2);

    b1.putLong(0, 0xFF);
    assertFalse(b1.equals(b2));
    b2.putLong(0, 0xFF);
    assertTrue(b1.equals(b2));
  }
}
