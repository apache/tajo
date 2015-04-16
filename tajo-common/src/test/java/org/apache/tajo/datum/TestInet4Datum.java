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

import org.apache.tajo.SerializeOption;
import org.apache.tajo.json.CommonGsonHelper;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestInet4Datum {

	@Before
	public void setUp() throws Exception {
	}
	
	@Test
	public final void testEquals() {
	  Inet4Datum ip1 = new Inet4Datum("192.168.0.1");
	  Inet4Datum ip2 = new Inet4Datum("192.168.0.1");
	  
	  assertEquals(ip1, ip2);
	  
	  Inet4Datum ip3 = new Inet4Datum(ip1.asByteArray());
	  assertEquals(ip1, ip3);
	  Inet4Datum ip4 = DatumFactory.createInet4(ip1.asByteArray());
	  assertEquals(ip1, ip4);
	}

  @Test
  public final void testGreaterThan() {
    Inet4Datum ip1 = new Inet4Datum("193.168.0.1");
    Inet4Datum ip2 = new Inet4Datum("192.168.100.1");

    assertEquals(ip1.compareTo(ip2), 1);
  }

  @Test
  public final void testLessThan() {
    Inet4Datum ip1 = new Inet4Datum("192.168.100.1");
    Inet4Datum ip2 = new Inet4Datum("193.168.0.1");

    assertEquals(ip1.compareTo(ip2), -1);
  }

	@Test
	public final void testAsByteArray() {
		byte[] bytes = {(byte) 0xA3, (byte) 0x98, 0x17, (byte) 0xDE};
		Inet4Datum ip = new Inet4Datum(bytes);
		assertTrue(Arrays.equals(bytes, ip.asByteArray()));
	}

	@Test
	public final void testAsChars() {
		Inet4Datum ip = new Inet4Datum("163.152.23.222");
		assertEquals("163.152.23.222", ip.asChars());
	}
	
	@Test
  public final void testSize() {
    Datum d = DatumFactory.createInet4("163.152.23.222");
    assertEquals(4, d.size());
  }
	
	@Test
	public final void testJson() {
		Datum d = DatumFactory.createInet4("163.152.163.152");
		String json = d.toJson(SerializeOption.GENERIC);
		Datum fromJson = CommonGsonHelper.fromJson(json, Datum.class);
		assertTrue(d.equalsTo(fromJson).asBool());
	}

  @Test
  public final void testAsTextBytes() {
    Datum d = DatumFactory.createInet4("163.152.23.222");
    assertArrayEquals(d.toString().getBytes(), d.asTextBytes());
  }
}
