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
}
