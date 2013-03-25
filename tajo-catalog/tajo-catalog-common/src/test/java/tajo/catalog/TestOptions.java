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

package tajo.catalog;

import org.junit.Test;
import tajo.catalog.proto.CatalogProtos.KeyValueSetProto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestOptions {
	@Test
	public final void testPutAndGet() {
		Options opts = new Options();
		opts.put("name", "abc");
		opts.put("delimiter", ",");
		
		assertEquals(",", opts.get("delimiter"));
		assertEquals("abc", opts.get("name"));
	}

	@Test
	public final void testGetProto() {		
		Options opts = new Options();
		opts.put("name", "abc");
		opts.put("delimiter", ",");
		
		KeyValueSetProto proto = opts.getProto();
		Options opts2 = new Options(proto);
		
		assertEquals(opts, opts2);
	}
	
	@Test
	public final void testDelete() {
		Options opts = new Options();
		opts.put("name", "abc");
		opts.put("delimiter", ",");
		
		assertEquals("abc", opts.get("name"));
		assertEquals("abc", opts.delete("name"));
		assertNull(opts.get("name"));
		
		Options opts2 = new Options(opts.getProto());
		assertNull(opts2.get("name"));
	}
}
