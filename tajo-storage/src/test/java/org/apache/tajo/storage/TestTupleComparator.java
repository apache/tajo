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

package org.apache.tajo.storage;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestTupleComparator {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public final void testCompare() {
    Schema schema = new Schema();
    schema.addColumn("col1", Type.INT4);
    schema.addColumn("col2", Type.INT4);
    schema.addColumn("col3", Type.INT4);
    schema.addColumn("col4", Type.INT4);
    schema.addColumn("col5", Type.TEXT);
    
    Tuple tuple1 = new VTuple(5);
    Tuple tuple2 = new VTuple(5);

    tuple1.put(
        new Datum[] {
        DatumFactory.createInt4(9),
        DatumFactory.createInt4(3),
        DatumFactory.createInt4(33),
        DatumFactory.createInt4(4),
        DatumFactory.createText("abc")});
    tuple2.put(
        new Datum[] {
        DatumFactory.createInt4(1),
        DatumFactory.createInt4(25),
        DatumFactory.createInt4(109),
        DatumFactory.createInt4(4),
        DatumFactory.createText("abd")});

    SortSpec sortKey1 = new SortSpec(schema.getColumn("col4"), true, false);
    SortSpec sortKey2 = new SortSpec(schema.getColumn("col5"), true, false);

    BaseTupleComparator tc = new BaseTupleComparator(schema,
        new SortSpec[] {sortKey1, sortKey2});
    assertEquals(-1, tc.compare(tuple1, tuple2));
    assertEquals(1, tc.compare(tuple2, tuple1));
  }
}
