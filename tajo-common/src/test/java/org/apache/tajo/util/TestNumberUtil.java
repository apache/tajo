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

import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestNumberUtil {
  @Test
  public void testNumberToAsciiBytes() {
    Random r = new Random(System.currentTimeMillis());

    Number n = r.nextInt();
    assertArrayEquals(String.valueOf(n.shortValue()).getBytes(), NumberUtil.toAsciiBytes(n.shortValue()));

    n = r.nextInt();
    assertArrayEquals(String.valueOf(n.intValue()).getBytes(), NumberUtil.toAsciiBytes(n.intValue()));

    n = r.nextLong();
    assertArrayEquals(String.valueOf(n.longValue()).getBytes(), NumberUtil.toAsciiBytes(n.longValue()));

    n = r.nextFloat();
    assertArrayEquals(String.valueOf(n.floatValue()).getBytes(), NumberUtil.toAsciiBytes(n.floatValue()));

    n = r.nextDouble();
    assertArrayEquals(String.valueOf(n.doubleValue()).getBytes(), NumberUtil.toAsciiBytes(n.doubleValue()));
  }

  @Test
  public void testParseInt() {
    int int1 = 0;
    byte[] bytes1 = Double.toString(int1).getBytes();
    assertEquals(int1, NumberUtil.parseInt(bytes1, 0, bytes1.length));

    int int2 = -7;
    byte[] bytes2 = Double.toString(int2).getBytes();
    assertEquals(int2, NumberUtil.parseInt(bytes2, 0, bytes2.length));

    int int3 = +128;
    byte[] bytes3 = Double.toString(int3).getBytes();
    assertEquals(int3, NumberUtil.parseInt(bytes3, 0, bytes3.length));

    int int4 = 4;
    byte[] bytes4 = Double.toString(int4).getBytes();
    assertEquals(int4, NumberUtil.parseInt(bytes4, 0, bytes4.length));

    byte[] bytes5 = "0123-456789".getBytes();
    assertEquals(-456, NumberUtil.parseInt(bytes5, 4, 4));

  }

  @Test
  public void testParseDouble() {
    double double1 = 2.0015E7;
    byte[] bytes1 = Double.toString(double1).getBytes();
    assertEquals(double1, NumberUtil.parseDouble(bytes1, 0, bytes1.length), 0.0);

    double double2 = 1.345E-7;
    byte[] bytes2 = Double.toString(double2).getBytes();
    assertEquals(double2, NumberUtil.parseDouble(bytes2, 0, bytes2.length), 0.0);

    double double3 = -1.345E-7;
    byte[] bytes3 = Double.toString(double3).getBytes();
    assertEquals(double3, NumberUtil.parseDouble(bytes3, 0, bytes3.length), 0.0);

    double double4 = 4;
    byte[] bytes4 = Double.toString(double4).getBytes();
    assertEquals(double4, NumberUtil.parseDouble(bytes4, 0, bytes4.length), 0.0);

    byte[] bytes5 = "0123456789.012345E012345".getBytes();
    assertEquals(6789.012345E01, NumberUtil.parseDouble(bytes5, 6, 14), 0.0);
  }
}
