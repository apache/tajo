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

package org.apache.tajo.engine.codegen;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestGeneratorAdapter {
  @Test
  public void testGetDescription() throws Exception {
    assertEquals("I", TajoGeneratorAdapter.getDescription(int.class));
    assertEquals("Ljava/lang/String;", TajoGeneratorAdapter.getDescription(String.class));
  }

  @Test
  public void getMethodDescription() throws Exception {
    assertEquals("(Lorg/apache/tajo/catalog/Schema;Lorg/apache/tajo/storage/Tuple;)Lorg/apache/tajo/datum/Datum;",
        TajoGeneratorAdapter.getMethodDescription(Datum.class, new Class[]{Schema.class, Tuple.class}));
  }
}
