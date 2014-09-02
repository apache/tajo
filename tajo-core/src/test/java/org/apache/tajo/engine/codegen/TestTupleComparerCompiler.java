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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.TupleComparatorImpl;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.offheap.OffHeapRowBlock;
import org.apache.tajo.storage.offheap.OffHeapRowBlockReader;
import org.apache.tajo.storage.offheap.UnSafeTuple;
import org.apache.tajo.storage.offheap.ZeroCopyTuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.apache.tajo.common.TajoDataTypes.Type.*;
import static org.apache.tajo.storage.offheap.TestOffHeapRowBlock.schema;
import static org.junit.Assert.assertTrue;

public class TestTupleComparerCompiler {
  private static TajoClassLoader classLoader;
  private static TupleComparerCompiler compiler;

  @BeforeClass
  public static void setUp() {
    classLoader = new TajoClassLoader();
    compiler = new TupleComparerCompiler(classLoader);
  }

  @AfterClass
  public static void tearDown() throws Throwable {
    compiler = null;

    classLoader.clean();
    classLoader = null;
  }

  private SortSpec [][] createSortSpecs(String columnName) {
    Column column = schema.getColumn(columnName);
    SortSpec [][] sortSpecList = new SortSpec[4][];
    sortSpecList[0] = new SortSpec[] {new SortSpec(column, true, false)};
    sortSpecList[1] = new SortSpec[] {new SortSpec(column, true, true)};
    sortSpecList[2] = new SortSpec[] {new SortSpec(column, false, false)};
    sortSpecList[3] = new SortSpec[] {new SortSpec(column, false, true)};
    return sortSpecList;
  }

  private TupleComparator [] createComparators(SortSpec [][] sortSpecs, boolean unsafeTuple) {
    TupleComparator [] comps = new TupleComparator[sortSpecs.length];
    for (int i = 0; i < sortSpecs.length; i++) {
      TupleComparatorImpl compImpl = new TupleComparatorImpl(schema, sortSpecs[i]);
      comps[i] = compiler.compile(compImpl, unsafeTuple);
    }

    return comps;
  }

  private void assertCompareAll(TupleComparator [] comps, SortSpec [][] sortSpecs, Tuple...tuples) {
    Preconditions.checkArgument(comps.length == sortSpecs.length);
    Preconditions.checkArgument(tuples.length == 5, "The number of tuples must be 5");

    for (int i = 0; i < comps.length; i++) {
      assertCompare(comps[i], sortSpecs[i], tuples);
    }
  }

  /**
   * First two tuples must be the same values for equality check.
   * @param tuples
   */
  private void assertCompare(TupleComparator comp, SortSpec [] sortSpecs, Tuple...tuples) {
    Preconditions.checkArgument(tuples.length == 5, "The number of tuples must be 5");

    if (sortSpecs[0].isAscending()) {
      assertTrue("Checking Equality", comp.compare(tuples[0], tuples[1]) == 0);
      assertTrue("Checking Less Than", comp.compare(tuples[0], tuples[2]) < 0);
      assertTrue("Checking Greater Than", comp.compare(tuples[2], tuples[0]) > 0);
    } else {
      assertTrue("Checking Equality", comp.compare(tuples[0], tuples[1]) == 0);
      assertTrue("Checking Less Than", comp.compare(tuples[0], tuples[2]) > 0);
      assertTrue("Checking Greater Than", comp.compare(tuples[2], tuples[0]) < 0);
    }

    if (sortSpecs[0].isNullFirst()) {
      assertTrue("Checking Greater Than", comp.compare(tuples[0], tuples[3]) > 0);
      assertTrue("Checking Greater Than", comp.compare(tuples[3], tuples[0]) < 0);
    } else {
      assertTrue("Checking Greater Than", comp.compare(tuples[0], tuples[3]) < 0);
      assertTrue("Checking Greater Than", comp.compare(tuples[3], tuples[0]) > 0);
    }

    assertTrue("Checking Null Equality", comp.compare(tuples[3], tuples[4]) == 0);
    assertTrue("Checking Null Equality", comp.compare(tuples[4], tuples[3]) == 0);
  }

  @Test
  public void testCompareOneBool() throws Exception {
    SortSpec [][] sortSpecs = createSortSpecs("col0");
    TupleComparator [] comps = createComparators(sortSpecs, false);

    Tuple t1 = new VTuple(schema.size());
    t1.put(0, DatumFactory.createBool(true));

    Tuple t2 = new VTuple(schema.size());
    t2.put(0, DatumFactory.createBool(true));

    Tuple t3 = new VTuple(schema.size());
    t3.put(0, DatumFactory.createBool(false));

    Tuple t4 = new VTuple(schema.size());
    t4.put(0, NullDatum.get());

    assertCompareAll(comps, sortSpecs, t1, t2, t3, t4, t4);
  }

  @Test
  public void testCompareOneInt() throws Exception {
    SortSpec [][] sortSpecs = createSortSpecs("col2");
    TupleComparator [] comps = createComparators(sortSpecs, false);

    Tuple t1 = new VTuple(schema.size());
    t1.put(2, DatumFactory.createInt2((short) 1));

    Tuple t2 = new VTuple(schema.size());
    t2.put(2, DatumFactory.createInt2((short) 1));

    Tuple t3 = new VTuple(schema.size());
    t3.put(2, DatumFactory.createInt2((short) 2));

    Tuple t4 = new VTuple(schema.size());
    t4.put(2, NullDatum.get());

    Tuple t5 = new VTuple(schema.size());
    t5.put(2, NullDatum.get());

    assertCompareAll(comps, sortSpecs, t1, t2, t3, t4, t5);
  }

  @Test
  public void testCompareTwoInts() throws Exception {
    SortSpec[] sortSpecs = new SortSpec[] {
        new SortSpec(new Column("col2", INT2)),
        new SortSpec(new Column("col3", INT4))};


    TupleComparatorImpl comparator = new TupleComparatorImpl(schema, sortSpecs);

    TajoClassLoader classLoader = new TajoClassLoader();

    TupleComparerCompiler compiler = new TupleComparerCompiler(classLoader);
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

    Tuple t5 = new VTuple(schema.size());
    t5.put(2, NullDatum.get());
    t5.put(3, DatumFactory.createInt4(2));

    Tuple t6 = new VTuple(schema.size());
    t6.put(2, DatumFactory.createInt2((short) 1));
    t6.put(3, NullDatum.get());

    assertCompare(compiled, sortSpecs, t1, t2, t3, t5, t5);
    assertCompare(compiled, sortSpecs, t1, t2, t4, t5, t5);
    assertCompare(compiled, sortSpecs, t1, t2, t5, t6, t6);
  }

  @Test
  public void testCompareOneFloat4() throws Exception {
    SortSpec [][] sortSpecs = createSortSpecs("col4");
    TupleComparator comps [] = createComparators(sortSpecs, false);

    Tuple t1 = new VTuple(schema.size());
    t1.put(4, DatumFactory.createFloat4(1.0f));

    Tuple t2 = new VTuple(schema.size());
    t2.put(4, DatumFactory.createFloat4(1.0f));

    Tuple t3 = new VTuple(schema.size());
    t3.put(4, DatumFactory.createFloat4(2.0f));

    Tuple t4 = new VTuple(schema.size());
    t4.put(4, NullDatum.get());

    assertCompareAll(comps, sortSpecs, t1, t2, t3, t4, t4);
  }

  @Test
  public void testCompareFloat4Float8() throws Exception {
    SortSpec[] sortSpecs = new SortSpec[] {
        new SortSpec(new Column("col4", FLOAT4)),
        new SortSpec(new Column("col5", FLOAT8))};

    TupleComparatorImpl comparator = new TupleComparatorImpl(schema, sortSpecs);

    TajoClassLoader classLoader = new TajoClassLoader();
    TupleComparerCompiler compiler = new TupleComparerCompiler(classLoader);
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

    Tuple t5 = new VTuple(schema.size());
    t5.put(4, DatumFactory.createFloat4(2.0f));
    t5.put(5, NullDatum.get());

    Tuple t6 = new VTuple(schema.size());
    t6.put(4, NullDatum.get());
    t6.put(5, DatumFactory.createFloat8(1.0f));

    assertCompare(compiled, sortSpecs, t1, t2, t3, t5, t5);
    assertCompare(compiled, sortSpecs, t1, t2, t4, t5, t5);
    assertCompare(compiled, sortSpecs, t1, t2, t5, t6, t6);
  }

  @Test
  public void testCompareText() throws Exception {
    SortSpec [][] sortSpecs = createSortSpecs("col6");
    TupleComparator [] comps = createComparators(sortSpecs, false);

    Tuple t1 = new VTuple(schema.size());
    t1.put(6, DatumFactory.createText("tajo"));

    Tuple t2 = new VTuple(schema.size());
    t2.put(6, DatumFactory.createText("tajo"));

    Tuple t3 = new VTuple(schema.size());
    t3.put(6, DatumFactory.createText("tazo"));

    Tuple t4 = new VTuple(schema.size());
    t4.put(6, NullDatum.get());

    assertCompareAll(comps, sortSpecs, t1, t2, t3, t4, t4);
  }

  @Test
  public void testCompareTextWithNull() throws Exception {
    SortSpec[] sortSpecs = new SortSpec[] {
        new SortSpec(new Column("col5", FLOAT8)),
        new SortSpec(new Column("col6", TEXT))};
    TupleComparatorImpl compImpl = new TupleComparatorImpl(schema, sortSpecs);
    TupleComparator comp = compiler.compile(compImpl, false);

    Tuple t1 = new VTuple(schema.size());
    t1.put(5, NullDatum.get());
    t1.put(6, DatumFactory.createText("ARGENTINA"));

    Tuple t2 = new VTuple(schema.size());
    t2.put(5, NullDatum.get());
    t2.put(6, DatumFactory.createText("ARGENTINA"));

    Tuple t3 = new VTuple(schema.size());
    t3.put(5, NullDatum.get());
    t3.put(6, DatumFactory.createText("CANADA"));

    Tuple t4 = new VTuple(schema.size());
    t4.put(5, NullDatum.get());
    t4.put(6, NullDatum.get());

    assertCompare(comp, sortSpecs, t1, t2, t3, t4, t4);
  }

  private void fillTextColumnToRowBlock(OffHeapRowBlock rowBlock, String text) {
    rowBlock.startRow();
    rowBlock.skipField(); // 0
    rowBlock.skipField(); // 1
    rowBlock.skipField(); // 2
    rowBlock.skipField(); // 3
    rowBlock.skipField(); // 4
    rowBlock.skipField(); // 5
    if (text != null) {
      rowBlock.putText(text);
    }
    rowBlock.endRow();
  }

  @Test
  public void testCompareTextInUnSafeTuple() throws Exception {
    SortSpec [][] sortSpecs = createSortSpecs("col6");
    TupleComparator [] comps = createComparators(sortSpecs, true);

    OffHeapRowBlock rowBlock = new OffHeapRowBlock(schema, 640);
    fillTextColumnToRowBlock(rowBlock, "tajo");
    fillTextColumnToRowBlock(rowBlock, "tajo");
    fillTextColumnToRowBlock(rowBlock, "tazo");
    fillTextColumnToRowBlock(rowBlock, null);

    List<UnSafeTuple> tuples = Lists.newArrayList();

    OffHeapRowBlockReader reader = new OffHeapRowBlockReader(rowBlock);

    reader.resetRowCursor();
    ZeroCopyTuple zcTuple = new ZeroCopyTuple();
    while(reader.next(zcTuple)) {
      tuples.add(zcTuple);
      zcTuple = new ZeroCopyTuple();
    }

    assertCompareAll(comps, sortSpecs, tuples.get(0), tuples.get(1), tuples.get(2), tuples.get(3), tuples.get(3));
    rowBlock.free();
  }
}