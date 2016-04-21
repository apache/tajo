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

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.tuple.memory.UnSafeTupleList;
import org.apache.tajo.unit.StorageUnit;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestUnSafeTuple {

  private static final Random rnd = new Random(-1);
  private static Schema schema;

  @BeforeClass
  public static void setupClass() {
    Column col0 = new Column("col0", Type.BOOLEAN);
    Column col1 = new Column("col1", Type.INT4);
    Column col2 = new Column("col2", Type.INT8);
    Column col3 = new Column("col3", Type.FLOAT4);
    Column col4 = new Column("col4", Type.FLOAT8);

    schema = SchemaBuilder.builder().addAll(new Column[]{col0, col1, col2, col3, col4}).build();
  }

  @Test
  public final void testMemoryPageAndValidation() {

    Datum[] datums = new Datum[]{
        DatumFactory.createBool(rnd.nextBoolean()),
        DatumFactory.createInt4(rnd.nextInt()),
        DatumFactory.createInt8(rnd.nextLong()),
        DatumFactory.createFloat4(rnd.nextFloat()),
        DatumFactory.createFloat8(rnd.nextDouble())};
    Tuple tuple = new VTuple(datums);

    int pageSize = StorageUnit.KB;
    UnSafeTupleList unSafeTupleList = new UnSafeTupleList(SchemaUtil.toDataTypes(schema), 100, StorageUnit.KB);
    assertEquals(0, unSafeTupleList.usedMem());
    assertEquals(0, unSafeTupleList.size());

    unSafeTupleList.addTuple(tuple);
    //get the memory bytes of tuple
    int tupleSize = unSafeTupleList.usedMem();
    assertEquals(1, unSafeTupleList.size());
    assertEquals(tuple, unSafeTupleList.get(0));

    unSafeTupleList.clear();
    assertEquals(0, unSafeTupleList.usedMem());
    assertEquals(0, unSafeTupleList.size());

    //test only 1 page
    int testCount = pageSize / tupleSize;
    Tuple[] tuples = new Tuple[testCount];

    for (int i = 0; i < testCount; i++) {
      datums = new Datum[]{
          DatumFactory.createBool(rnd.nextBoolean()),
          DatumFactory.createInt4(rnd.nextInt()),
          DatumFactory.createInt8(rnd.nextLong()),
          DatumFactory.createFloat4(rnd.nextFloat()),
          DatumFactory.createFloat8(rnd.nextDouble())};
      tuples[i] = new VTuple(datums);
      unSafeTupleList.addTuple(tuples[i]);
    }

    assertEquals(testCount, unSafeTupleList.size());
    assertEquals(tupleSize * testCount, unSafeTupleList.usedMem());

    for (int i = 0; i < testCount; i++) {
      assertEquals(tuples[i], unSafeTupleList.get(i));
    }

    unSafeTupleList.release();
  }
}
