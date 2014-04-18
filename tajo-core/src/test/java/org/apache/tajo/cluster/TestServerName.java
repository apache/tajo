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

package org.apache.tajo.cluster;

import org.junit.Test;
import org.apache.tajo.master.cluster.ServerName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestServerName {

	@Test
	public void testServerNameStringInt() {
		ServerName server = new ServerName("ex1.com",50030);
		assertEquals("ex1.com", server.getHostname());
		assertEquals(50030, server.getPort());
	}

	@Test
	public void testServerNameString() {
		ServerName server = new ServerName("ex1.com:50030");
		assertEquals("ex1.com", server.getHostname());
		assertEquals(50030, server.getPort());
	}

	@Test
	public void testParseHostname() {
		assertEquals("ex1.com",ServerName.parseHostname("ex1.com:50030"));
	}

	@Test
	public void testParsePort() {
		assertEquals(50030,ServerName.parsePort("ex1.com:50030"));
	}

	@Test
	public void testToString() {
		ServerName server = new ServerName("ex1.com",50030);
		assertEquals("ex1.com:50030", server.toString());
	}

	@Test
	public void testGetServerName() {
		ServerName server = new ServerName("ex1.com",50030);
		assertEquals("ex1.com:50030", server.getServerName());
	}

	@Test
	public void testGetHostname() {
		ServerName server = new ServerName("ex1.com",50030);
		assertEquals("ex1.com", server.getHostname());
	}

	@Test
	public void testGetPort() {
		ServerName server = new ServerName("ex1.com",50030);
		assertEquals(50030, server.getPort());
	}

	@Test
	public void testGetServerNameStringInt() {
		assertEquals("ex2.com:50030",ServerName.getServerName("ex2.com", 50030));
	}

	@Test
	public void testCompareTo() {
		ServerName s1 = new ServerName("ex1.com:50030");
		ServerName s2 = new ServerName("ex1.com:60030");
		
		assertTrue(s1.compareTo(s2) < 0);
		assertTrue(s2.compareTo(s1) > 0);
		
		ServerName s3 = new ServerName("ex1.com:50030");
		assertTrue(s1.compareTo(s3) == 0);
		
		ServerName s4 = new ServerName("ex2.com:50030");
		assertTrue(s1.compareTo(s4) < 0);
		assertTrue(s4.compareTo(s1) > 0);
	}

  @Test (expected = IllegalArgumentException.class)
  public void testException() {
    new ServerName("ex1.com");
  }
}
