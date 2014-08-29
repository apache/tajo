/***
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

import com.google.common.primitives.Bytes;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.TupleComparatorImpl;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.directmem.RowOrientedRowBlock;
import org.apache.tajo.storage.directmem.TestRowOrientedRowBlock;
import org.apache.tajo.storage.directmem.UnSafeTuple;
import org.apache.tajo.storage.directmem.UnSafeTupleTextComparator;
import org.junit.Test;

import static org.apache.tajo.common.TajoDataTypes.Type.*;
import static org.apache.tajo.storage.directmem.TestRowOrientedRowBlock.schema;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class TestTupleComparatorCompiler {
  @Test
  public void testCompareOneInt() throws Exception {
    SortSpec[] sortSpecs = new SortSpec[] {
        new SortSpec(new Column("col2", INT2))};


    TupleComparatorImpl comparator = new TupleComparatorImpl(schema, sortSpecs);

    TajoClassLoader classLoader = new TajoClassLoader();

    TupleComparatorCompiler compiler = new TupleComparatorCompiler(classLoader);
    TupleComparator compiled = compiler.compile(comparator, false);

    Tuple t1 = new VTuple(schema.size());
    t1.put(2, DatumFactory.createInt2((short) 1));

    Tuple t2 = new VTuple(schema.size());
    t2.put(2, DatumFactory.createInt2((short) 1));

    Tuple t3 = new VTuple(schema.size());
    t3.put(2, DatumFactory.createInt2((short) 2));

    assertCompare(compiled, t1, t2, t3);
  }

  @Test
  public void testCompareTwoInts() throws Exception {
    SortSpec[] sortSpecs = new SortSpec[] {
        new SortSpec(new Column("col2", INT2)),
        new SortSpec(new Column("col3", INT4))};


    TupleComparatorImpl comparator = new TupleComparatorImpl(schema, sortSpecs);

    TajoClassLoader classLoader = new TajoClassLoader();

    TupleComparatorCompiler compiler = new TupleComparatorCompiler(classLoader);
    TupleComparator compiled = compiler.compile(comparator, false);

    Tuple t1 = new VTuple(schema.size());
    t1.put(2, DatumFactory.createInt2((short) 1));
    t1.put(3, DatumFactory.createInt4(1));

    Tuple t2 = new VTuple(schema.size());
    t2.put(2, DatumFactory.createInt2((short) 1));
    t2.put(3, DatumFactory.createInt4(1));

    Tuple t3 = new VTuple(schema.size());
    t3.put(2, DatumFactory.createInt2((short) 2));
    t3.put(3, DatumFactory.createInt4(1));

    Tuple t4 = new VTuple(schema.size());
    t4.put(2, DatumFactory.createInt2((short) 1));
    t4.put(3, DatumFactory.createInt4(2));

    assertCompare(compiled, t1, t2, t3);
    assertCompare(compiled, t1, t2, t4);
  }

  /**
   * First two tuples must be the same values for equality check.
   * @param tuples
   */
  private void assertCompare(TupleComparator comp, Tuple...tuples) {
    assertTrue("Checking Equality", comp.compare(tuples[0], tuples[1]) == 0);
    assertTrue("Checking Less Than", comp.compare(tuples[0], tuples[2]) < 0);
    assertTrue("Checking Greater Than", comp.compare(tuples[2], tuples[0]) > 0);
  }

  @Test
  public void testCompareOneFloat4() throws Exception {
    SortSpec[] sortSpecs = new SortSpec[] {
        new SortSpec(new Column("col4", FLOAT4))};

    TupleComparatorImpl comparator = new TupleComparatorImpl(schema, sortSpecs);

    TajoClassLoader classLoader = new TajoClassLoader();
    TupleComparatorCompiler compiler = new TupleComparatorCompiler(classLoader);
    TupleComparator compiled = compiler.compile(comparator, false);

    Tuple t1 = new VTuple(schema.size());
    t1.put(4, DatumFactory.createFloat4(1.0f));

    Tuple t2 = new VTuple(schema.size());
    t2.put(4, DatumFactory.createFloat4(1.0f));

    Tuple t3 = new VTuple(schema.size());
    t3.put(4, DatumFactory.createFloat4(2.0f));

    assertCompare(compiled, t1, t2, t3);
  }

  @Test
  public void testCompareFloat4Float8() throws Exception {
    SortSpec[] sortSpecs = new SortSpec[] {
        new SortSpec(new Column("col4", FLOAT4)),
        new SortSpec(new Column("col5", FLOAT8))};

    TupleComparatorImpl comparator = new TupleComparatorImpl(schema, sortSpecs);

    TajoClassLoader classLoader = new TajoClassLoader();
    TupleComparatorCompiler compiler = new TupleComparatorCompiler(classLoader);
    TupleComparator compiled = compiler.compile(comparator, false);

    Tuple t1 = new VTuple(schema.size());
    t1.put(4, DatumFactory.createFloat4(1.0f));
    t1.put(5, DatumFactory.createFloat8(1.0f));

    Tuple t2 = new VTuple(schema.size());
    t2.put(4, DatumFactory.createFloat4(1.0f));
    t2.put(5, DatumFactory.createFloat8(1.0f));

    Tuple t3 = new VTuple(schema.size());
    t3.put(4, DatumFactory.createFloat4(1.0f));
    t3.put(5, DatumFactory.createFloat8(2.0f));

    Tuple t4 = new VTuple(schema.size());
    t4.put(4, DatumFactory.createFloat4(2.0f));
    t4.put(5, DatumFactory.createFloat8(1.0f));

    assertCompare(compiled, t1, t2, t3);
    assertCompare(compiled, t1, t2, t4);
  }

  @Test
  public void testCompareText() throws Exception {
    SortSpec[] sortSpecs = new SortSpec[] {
        new SortSpec(new Column("col6", TEXT))};

    TupleComparatorImpl comparator = new TupleComparatorImpl(schema, sortSpecs);

    TajoClassLoader classLoader = new TajoClassLoader();
    TupleComparatorCompiler compiler = new TupleComparatorCompiler(classLoader);
    TupleComparator compiled = compiler.compile(comparator, false);

    Tuple t1 = new VTuple(schema.size());
    t1.put(6, DatumFactory.createText("tajo"));

    Tuple t2 = new VTuple(schema.size());
    t2.put(6, DatumFactory.createText("tajo"));

    Tuple t3 = new VTuple(schema.size());
    t3.put(6, DatumFactory.createText("tazo"));

    assertCompare(compiled, t1, t2, t3);
  }

  @Test
  public void testCompareTextInUnSafeTuple() throws Exception {
    SortSpec[] sortSpecs = new SortSpec[] {
        new SortSpec(new Column("col6", TEXT))};

    TupleComparatorImpl comparator = new TupleComparatorImpl(schema, sortSpecs);

    TajoClassLoader classLoader = new TajoClassLoader();
    TupleComparatorCompiler compiler = new TupleComparatorCompiler(classLoader);
    TupleComparator compiled = compiler.compile(comparator, true);

    RowOrientedRowBlock rowBlock = TestRowOrientedRowBlock.createRowBlock(10);

    UnSafeTuple t1 = new UnSafeTuple();
    assertTrue(rowBlock.next(t1));

    UnSafeTuple t2 = new UnSafeTuple(128, SchemaUtil.toDataTypes(schema));
    t2.copyFrom(t1);

    UnSafeTuple t3 = new UnSafeTuple();
    assertTrue(rowBlock.next(t3));

    assertCompare(compiled, t1, t2, t3);
  }
}