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

package org.apache.tajo.datum;

import org.apache.tajo.util.Bytes;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestBytesDatum {

  @Test
  public final void testAsChars() {
    Datum d = DatumFactory.createBlob("12345".getBytes());
    assertEquals("12345", d.asChars());
  }
  
  @Test
  public final void testSize() {
    Datum d = DatumFactory.createBlob("12345".getBytes());
    assertEquals(5, d.size());
  }

  @Test
  public final void testAsTextBytes() {
    Datum d = DatumFactory.createBlob("12345".getBytes());
    assertArrayEquals(d.toString().getBytes(), d.asTextBytes());

    byte[] bytes = "12345".getBytes();
    d = DatumFactory.createBlob(bytes, 0, 1);
    assertEquals(d.toString(), "1");
  }

  @Test
  public final void testAsBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(14);
    buffer.putShort((short)1);
    buffer.putInt(123);
    buffer.putLong(123456);
    buffer.flip();
    byte[] bytes = Bytes.getBytes(buffer);

    Datum d = new BlobDatum(buffer);
    assertArrayEquals(bytes, d.asByteArray());
    buffer.clear();

    byte[] bytes1 = new byte[1024];
    System.arraycopy(bytes, 0, bytes1, 0, bytes.length);
    d = new BlobDatum(bytes1, 0, bytes.length);
    assertArrayEquals(bytes, d.asByteArray());
  }
}
