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

package tajo.storage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.catalog.Schema;
import tajo.catalog.SortSpec;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;

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
    schema.addColumn("col1", DataType.INT);
    schema.addColumn("col2", DataType.INT);
    schema.addColumn("col3", DataType.INT);
    schema.addColumn("col4", DataType.INT);
    schema.addColumn("col5", DataType.STRING);
    
    Tuple tuple1 = new VTuple(5);
    Tuple tuple2 = new VTuple(5);

    tuple1.put(
        new Datum[] {
        DatumFactory.createInt(9), 
        DatumFactory.createInt(3),
        DatumFactory.createInt(33), 
        DatumFactory.createInt(4),
        DatumFactory.createString("abc")});
    tuple2.put(
        new Datum[] {
        DatumFactory.createInt(1), 
        DatumFactory.createInt(25),
        DatumFactory.createInt(109),
        DatumFactory.createInt(4),
        DatumFactory.createString("abd")});

    SortSpec sortKey1 = new SortSpec(schema.getColumn("col4"), true, false);
    SortSpec sortKey2 = new SortSpec(schema.getColumn("col5"), true, false);

    TupleComparator tc = new TupleComparator(schema,
        new SortSpec[] {sortKey1, sortKey2});
    assertEquals(-1, tc.compare(tuple1, tuple2));
    assertEquals(1, tc.compare(tuple2, tuple1));
  }
}
