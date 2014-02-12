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

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestBytes {

  @Test
  public void testAsciiBytes() {
    String asciiText = "abcde 12345 ABCDE";
    assertArrayEquals(asciiText.getBytes(), Bytes.toASCIIBytes(asciiText.toCharArray()));
  }

  @Test
  public void testSplitBytes() {
    String text = "abcde|12345|ABCDE";
    char separatorChar = '|';

    String[] textArray = StringUtils.splitPreserveAllTokens(text, separatorChar);
    byte[][] bytesArray =  Bytes.splitPreserveAllTokens(text.getBytes(), separatorChar);

    assertEquals(textArray.length, bytesArray.length);
    for (int i = 0; i < textArray.length; i++){
      assertArrayEquals(textArray[i].getBytes(), bytesArray[i]);
    }
  }

  @Test
  public void testSplitProjectionBytes() {
    String text = "abcde|12345|ABCDE";
    int[] target = new int[]{ 1 };
    char separatorChar = '|';

    String[] textArray = StringUtils.splitPreserveAllTokens(text, separatorChar);
    byte[][] bytesArray =  Bytes.splitPreserveAllTokens(text.getBytes(), separatorChar, target);

    assertEquals(textArray.length, bytesArray.length);

    assertNull(bytesArray[0]);
    assertNotNull(bytesArray[1]);
    assertArrayEquals(textArray[1].getBytes(), bytesArray[1]);
    assertNull(bytesArray[2]);
  }

  @Test
  public void testParseInt() {
    int int1 = 0;
    byte[] bytes1 = Double.toString(int1).getBytes();
    assertEquals(int1, Bytes.parseInt(bytes1, 0, bytes1.length));

    int int2 = -7;
    byte[] bytes2 = Double.toString(int2).getBytes();
    assertEquals(int2, Bytes.parseInt(bytes2, 0, bytes2.length));

    int int3 = +128;
    byte[] bytes3 = Double.toString(int3).getBytes();
    assertEquals(int3, Bytes.parseInt(bytes3, 0, bytes3.length));

    int int4 = 4;
    byte[] bytes4 = Double.toString(int4).getBytes();
    assertEquals(int4, Bytes.parseInt(bytes4, 0, bytes4.length));

    byte[] bytes5 = "0123-456789".getBytes();
    assertEquals(-456, Bytes.parseInt(bytes5, 4, 4));

  }

  @Test
  public void testParseDouble() {
    double double1 = 2.0015E7;
    byte[] bytes1 = Double.toString(double1).getBytes();
    assertEquals(double1, Bytes.parseDouble(bytes1, 0, bytes1.length), 0.0);

    double double2 = 1.345E-7;
    byte[] bytes2 = Double.toString(double2).getBytes();
    assertEquals(double2, Bytes.parseDouble(bytes2, 0, bytes2.length), 0.0);

    double double3 = -1.345E-7;
    byte[] bytes3 = Double.toString(double3).getBytes();
    assertEquals(double3, Bytes.parseDouble(bytes3, 0, bytes3.length), 0.0);

    double double4 = 4;
    byte[] bytes4 = Double.toString(double4).getBytes();
    assertEquals(double4, Bytes.parseDouble(bytes4, 0, bytes4.length), 0.0);

    byte[] bytes5 = "0123456789.012345E012345".getBytes();
    assertEquals(6789.012345E01, Bytes.parseDouble(bytes5, 6, 14), 0.0);

  }

}
