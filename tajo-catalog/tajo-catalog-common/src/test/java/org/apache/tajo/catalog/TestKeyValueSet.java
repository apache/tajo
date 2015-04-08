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

package org.apache.tajo.catalog;

import org.apache.tajo.SerializeOption;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestKeyValueSet {

	@Test
	public final void testPutAndGetString() {
		KeyValueSet opts = new KeyValueSet();
		opts.set("k1", "v1");
		opts.set("k2", "v2");
		
		assertEquals("v1", opts.get("k1"));
		assertEquals("v2", opts.get("k2"));
    assertEquals("default", opts.get("k3", "default"));
	}

  @Test
  public final void testPutAndGetBool() {
    KeyValueSet opts = new KeyValueSet();
    opts.setBool("k1", true);
    opts.setBool("k2", false);

    assertEquals(true, opts.getBool("k1"));
    assertEquals(false, opts.getBool("k2"));
    assertEquals(true, opts.getBool("k3", true));
  }

  @Test
  public final void testPutAndGetInt() {
    KeyValueSet opts = new KeyValueSet();
    opts.setInt("k1", 1980);
    opts.setInt("k2", 401);

    assertEquals(1980, opts.getInt("k1"));
    assertEquals(401, opts.getInt("k2"));
    assertEquals(2020, opts.getInt("k3", 2020));
  }

  @Test
  public final void testPutAndGetLong() {
    KeyValueSet opts = new KeyValueSet();
    opts.setLong("k1", 1980);
    opts.setLong("k2", 401);

    assertEquals(1980, opts.getLong("k1"));
    assertEquals(401, opts.getLong("k2"));
    assertEquals(2020, opts.getLong("k3", 2020l));
  }

  @Test
  public final void testPutAndGetFloat() {
    KeyValueSet opts = new KeyValueSet();
    opts.setFloat("k1", 1980.4f);
    opts.setFloat("k2", 401.150f);

    assertTrue(1980.4f == opts.getFloat("k1"));
    assertTrue(401.150f == opts.getFloat("k2"));
    assertTrue(3.14f == opts.getFloat("k3", 3.14f));
  }

	@Test
	public final void testGetProto() {		
		KeyValueSet opts = new KeyValueSet();
		opts.set("name", "abc");
		opts.set("delimiter", ",");
		
		PrimitiveProtos.KeyValueSetProto proto = opts.getProto(SerializeOption.GENERIC);
		KeyValueSet opts2 = new KeyValueSet(proto);
		
		assertEquals(opts, opts2);
	}
	
	@Test
	public final void testRemove() {
		KeyValueSet opts = new KeyValueSet();
		opts.set("name", "abc");
		opts.set("delimiter", ",");
		
		assertEquals("abc", opts.get("name"));
		assertEquals("abc", opts.remove("name"));
    try {
		  opts.get("name");
      assertTrue(false);
    } catch (IllegalArgumentException iae) {
      assertTrue(true);
    }
		
		KeyValueSet opts2 = new KeyValueSet(opts.getProto(SerializeOption.GENERIC));
    try {
      opts2.get("name");
      assertTrue(false);
    } catch (IllegalArgumentException iae) {
      assertTrue(true);
    }
	}
}
