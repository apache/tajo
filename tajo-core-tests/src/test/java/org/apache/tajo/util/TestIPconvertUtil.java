/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.tajo.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class TestIPconvertUtil {
  @Test
  public void testIntConvertMethods() {
    assertEquals(16909060, IPconvertUtil.ipstr2int("1.2.3.4"));
    assertEquals(-1062731699, IPconvertUtil.ipstr2int("192.168.0.77"));

    assertEquals("1.2.3.4", IPconvertUtil.int2ipstr(16909060));
    assertEquals("192.168.0.77", IPconvertUtil.int2ipstr(-1062731699));
  }

  @Test
  public void testBytesConvertMethods() {
    byte [] addr = new byte[4];

    addr[0] = 1; addr[1] = 2; addr[2] = 3; addr[3] = 4;
    assertArrayEquals(addr, IPconvertUtil.ipstr2bytes("1.2.3.4"));
    assertEquals("1.2.3.4", IPconvertUtil.bytes2ipstr(addr));

    addr[0] = (byte)192; addr[1] = (byte)168; addr[2] = 0; addr[3] = 77;
    assertArrayEquals(addr, IPconvertUtil.ipstr2bytes("192.168.0.77"));
    assertEquals("192.168.0.77", IPconvertUtil.bytes2ipstr(addr));
  }
}
