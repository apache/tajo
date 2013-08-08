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

import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.json.CommonGsonHelper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBytesDatum {

  @Test
  public final void testType() {
    Datum d = DatumFactory.createBlob("12345".getBytes());
    assertEquals(Type.BLOB, d.type());
  }
  
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
  public final void testJson() {
	  Datum d = DatumFactory.createBlob("12345".getBytes());
	  String json = d.toJson();
	  Datum fromJson = CommonGsonHelper.fromJson(json, Datum.class);
	  assertTrue(d.equalsTo(fromJson).asBool());
  }
}
