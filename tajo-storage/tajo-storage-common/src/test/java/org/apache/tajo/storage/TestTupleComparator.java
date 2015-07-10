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
import org.apache.tajo.datum.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    
    VTuple tuple1 = new VTuple(
        new Datum[] {
        DatumFactory.createInt4(9),
        DatumFactory.createInt4(3),
        DatumFactory.createInt4(33),
        DatumFactory.createInt4(4),
        DatumFactory.createText("abc")});
    VTuple tuple2 = new VTuple(
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

  @Test
  public void testNullFirst() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);

    VTuple tuple1 = new VTuple(2);
    tuple1.put(0, new Int4Datum(1));
    tuple1.put(1, new TextDatum("111"));

    VTuple nullTuple = new VTuple(2);
    nullTuple.put(0, new Int4Datum(2));
    nullTuple.put(1, NullDatum.get());

    VTuple tuple3 = new VTuple(2);
    tuple3.put(0, new Int4Datum(3));
    tuple3.put(1, new TextDatum("333"));

    List<Tuple> tupleList = new ArrayList<Tuple>();
    tupleList.add(tuple1);
    tupleList.add(nullTuple);
    tupleList.add(tuple3);

    SortSpec sortSpecAsc = new SortSpec(schema.getColumn(1), true, true);
    BaseTupleComparator ascComp = new BaseTupleComparator(schema, new SortSpec[]{sortSpecAsc});
    Collections.sort(tupleList, ascComp);

    assertEquals(nullTuple, tupleList.get(0));
    assertEquals(tuple1, tupleList.get(1));
    assertEquals(tuple3, tupleList.get(2));

    SortSpec sortSpecDesc = new SortSpec(schema.getColumn(1), false, true);
    BaseTupleComparator descComp = new BaseTupleComparator(schema, new SortSpec[]{sortSpecDesc});
    Collections.sort(tupleList, descComp);

    assertEquals(tuple3, tupleList.get(0));
    assertEquals(tuple1, tupleList.get(1));
    assertEquals(nullTuple, tupleList.get(2));
  }

  @Test
  public void testNullLast() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);

    VTuple tuple1 = new VTuple(2);
    tuple1.put(0, new Int4Datum(1));
    tuple1.put(1, new TextDatum("111"));

    VTuple nullTuple = new VTuple(2);
    nullTuple.put(0, new Int4Datum(2));
    nullTuple.put(1, NullDatum.get());

    VTuple tuple3 = new VTuple(2);
    tuple3.put(0, new Int4Datum(3));
    tuple3.put(1, new TextDatum("333"));

    List<Tuple> tupleList = new ArrayList<Tuple>();
    tupleList.add(tuple1);
    tupleList.add(nullTuple);
    tupleList.add(tuple3);

    SortSpec sortSpecAsc = new SortSpec(schema.getColumn(1), true, false);
    BaseTupleComparator ascComp = new BaseTupleComparator(schema, new SortSpec[]{sortSpecAsc});

    Collections.sort(tupleList, ascComp);

    assertEquals(tuple1, tupleList.get(0));
    assertEquals(tuple3, tupleList.get(1));
    assertEquals(nullTuple, tupleList.get(2));

    SortSpec sortSpecDesc = new SortSpec(schema.getColumn(1), false, false);
    BaseTupleComparator descComp = new BaseTupleComparator(schema, new SortSpec[]{sortSpecDesc});

    Collections.sort(tupleList, descComp);

    assertEquals(nullTuple, tupleList.get(0));
    assertEquals(tuple3, tupleList.get(1));
    assertEquals(tuple1, tupleList.get(2));
  }
}
