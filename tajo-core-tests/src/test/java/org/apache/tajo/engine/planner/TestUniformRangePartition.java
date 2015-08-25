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

package org.apache.tajo.engine.planner;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.BaseTupleComparator;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.VTuple;
import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestUniformRangePartition {

  @Test
  public void testPartitionForINT2Asc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.INT2);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createInt2((short) 1));
    e.put(0, DatumFactory.createInt2((short) 30000));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForINT2Desc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.INT2);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);
    sortSpecs[0].setDescOrder();

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createInt2((short) 30000));
    e.put(0, DatumFactory.createInt2((short) 1));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForINT4Asc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.INT4);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createInt4(1));
    e.put(0, DatumFactory.createInt4(10000));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForINT4Desc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.INT4);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);
    sortSpecs[0].setDescOrder();

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createInt4(10000));
    e.put(0, DatumFactory.createInt4(1));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForINT8Asc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.INT8);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createInt8(1));
    e.put(0, DatumFactory.createInt8(10000));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForInt8Desc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.INT8);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);
    sortSpecs[0].setDescOrder();

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createInt8(10000));
    e.put(0, DatumFactory.createInt8(1));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForFloat4Asc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.FLOAT4);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createFloat4((float) 1.0));
    e.put(0, DatumFactory.createFloat4((float) 10000.0));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForFloat4Desc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.FLOAT4);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);
    sortSpecs[0].setDescOrder();

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createFloat4((float) 10000.0));
    e.put(0, DatumFactory.createFloat4((float) 1.0));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForFloat8Asc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.FLOAT8);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createFloat8(1.0));
    e.put(0, DatumFactory.createFloat8(10000.0));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForFloat8Desc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.FLOAT8);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);
    sortSpecs[0].setDescOrder();

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createFloat8((float) 10000.0));
    e.put(0, DatumFactory.createFloat8((float) 1.0));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  /**
   * It verify overflow and increment in normal case.
   */
  @Test
  public void testIncrementOfText() {
    Schema schema = new Schema()
        .addColumn("l_returnflag", Type.TEXT)
        .addColumn("l_linestatus", Type.TEXT);

    SortSpec[] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(2);
    s.put(0, DatumFactory.createText("A"));
    s.put(1, DatumFactory.createText("A"));
    VTuple e = new VTuple(2);
    e.put(0, DatumFactory.createText("D"));
    e.put(1, DatumFactory.createText("C"));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    assertEquals(12, partitioner.getTotalCardinality().intValue());

    String [] result = new String[12];
    result[0] = "AA";
    result[1] = "AB";
    result[2] = "AC";
    result[3] = "BA";
    result[4] = "BB";
    result[5] = "BC";
    result[6] = "CA";
    result[7] = "CB";
    result[8] = "CC";
    result[9] = "DA";
    result[10] = "DB";
    result[11] = "DC";

    Tuple end = partitioner.increment(s, BigInteger.valueOf(1), 1);
    assertEquals("A", end.getText(0));
    assertEquals("B", end.getText(1));
    for (int i = 2; i < 11; i++ ) {
      end = partitioner.increment(end, BigInteger.valueOf(1), 1);
      assertEquals(result[i].charAt(0), end.getText(0).charAt(0));
      assertEquals(result[i].charAt(1), end.getText(1).charAt(0));
    }
  }

  /**
   * It verify overflow with the number that exceeds the last digit.
   */
  @Test
  public void testIncrementOfText2() {
    Schema schema = new Schema()
        .addColumn("l_returnflag", Type.TEXT)
        .addColumn("l_linestatus", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(2);
    s.put(0, DatumFactory.createText("A"));
    s.put(1, DatumFactory.createText("A"));
    VTuple e = new VTuple(2);
    e.put(0, DatumFactory.createText("D"));
    e.put(1, DatumFactory.createText("C"));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    assertEquals(12, partitioner.getTotalCardinality().intValue());

    String [] result = new String[12];
    result[0] = "AA";
    result[1] = "AB";
    result[2] = "AC";
    result[3] = "BA";
    result[4] = "BB";
    result[5] = "BC";
    result[6] = "CA";
    result[7] = "CB";
    result[8] = "CC";
    result[9] = "DA";
    result[10] = "DB";
    result[11] = "DC";

    Tuple end = partitioner.increment(s, BigInteger.valueOf(6), 1);
    assertEquals("C", end.getText(0));
    assertEquals("A", end.getText(1));
    end = partitioner.increment(end, BigInteger.valueOf(5), 1);
    assertEquals("D", end.getText(0));
    assertEquals("C", end.getText(1));
  }

  /**
   * It verify the case where two or more digits are overflow.
   */
  @Test
  public void testIncrementOfText3() {
    Schema schema = new Schema()
        .addColumn("l_returnflag", Type.TEXT)
        .addColumn("l_linestatus", Type.TEXT)
        .addColumn("final", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(3);
    s.put(0, DatumFactory.createText("A"));
    s.put(1, DatumFactory.createText("A"));
    s.put(2, DatumFactory.createText("A"));
    VTuple e = new VTuple(3);
    e.put(0, DatumFactory.createText("D")); //  4
    e.put(1, DatumFactory.createText("B")); //  2
    e.put(2, DatumFactory.createText("C")); // x3 = 24

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    assertEquals(24, partitioner.getTotalCardinality().intValue());

    Tuple overflowBefore = partitioner.increment(s, BigInteger.valueOf(5), 2);
    assertEquals("A", overflowBefore.getText(0));
    assertEquals("B", overflowBefore.getText(1));
    assertEquals("C", overflowBefore.getText(2));
    Tuple overflowed = partitioner.increment(overflowBefore, BigInteger.valueOf(1), 2);
    assertEquals("B", overflowed.getText(0));
    assertEquals("A", overflowed.getText(1));
    assertEquals("A", overflowed.getText(2));
  }

  @Test
  public void testIncrementOfUnicode() {
    Schema schema = new Schema()
        .addColumn("col1", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(1);
    s.put(0, DatumFactory.createText("가가가"));
    VTuple e = new VTuple(1);
    e.put(0, DatumFactory.createText("하하하"));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    BaseTupleComparator comp = new BaseTupleComparator(schema, sortSpecs);

    Tuple tuple = s;
    Tuple prevTuple = null;
    for (int i = 0; i < 100; i++) {
      tuple = partitioner.increment(tuple, BigInteger.valueOf(30000), 0);
      if (prevTuple != null) {
        assertTrue("prev=" + prevTuple + ", current=" + tuple, comp.compare(prevTuple, tuple) < 0);
      }
      prevTuple = tuple;
    }
  }

  @Test
  public void testIncrementOfUnicodeOneCharSinglePartition() {
    Schema schema = new Schema()
        .addColumn("col1", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(1);
    s.put(0, DatumFactory.createText("가"));
    VTuple e = new VTuple(1);
    e.put(0, DatumFactory.createText("다"));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 1;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testIncrementOfUnicodeOneCharMultiPartition() {
    Schema schema = new Schema()
        .addColumn("col1", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(1);
    s.put(0, DatumFactory.createText("가"));
    VTuple e = new VTuple(1);
    e.put(0, DatumFactory.createText("꽥"));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 8;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForUnicodeTextAsc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createText("가가가"));
    e.put(0, DatumFactory.createText("하하하"));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForUnicodeDiffLenBeginTextAsc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createText("가"));
    e.put(0, DatumFactory.createText("하하하"));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForUnicodeDiffLenEndTextAsc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createText("가가가"));
    e.put(0, DatumFactory.createText("하"));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForUnicodeTextDesc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);
    sortSpecs[0].setDescOrder();

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createText("하하하"));
    e.put(0, DatumFactory.createText("가가가"));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForUnicodeDiffLenBeginTextDesc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);
    sortSpecs[0].setDescOrder();

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createText("하"));
    e.put(0, DatumFactory.createText("가가가"));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForUnicodeDiffLenEndTextDesc() {
    Schema schema = new Schema()
        .addColumn("col1", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);
    sortSpecs[0].setDescOrder();

    VTuple s = new VTuple(1);
    VTuple e = new VTuple(1);
    s.put(0, DatumFactory.createText("하"));
    e.put(0, DatumFactory.createText("가가가"));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    int partNum = 64;
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testIncrementOfInt8() {
    Schema schema = new Schema()
        .addColumn("l_orderkey", Type.INT8)
        .addColumn("l_linenumber", Type.INT8);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(2);
    s.put(0, DatumFactory.createInt8(10));
    s.put(1, DatumFactory.createInt8(20));
    VTuple e = new VTuple(2);
    e.put(0, DatumFactory.createInt8(19));
    e.put(1, DatumFactory.createInt8(39));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    assertEquals(200, partitioner.getTotalCardinality().longValue());

    Tuple range2 = partitioner.increment(s, BigInteger.valueOf(100), 1);
    assertEquals(15, range2.getInt4(0));
    assertEquals(20, range2.getInt4(1));
    Tuple range3 = partitioner.increment(range2, BigInteger.valueOf(99), 1);
    assertEquals(19, range3.getInt4(0));
    assertEquals(39, range3.getInt4(1));
  }

  @Test public void testIncrementOfInt8AndFinal() {
    Schema schema = new Schema()
        .addColumn("l_orderkey", Type.INT8)
        .addColumn("l_linenumber", Type.INT8)
        .addColumn("final", Type.INT8);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(3);
    s.put(0, DatumFactory.createInt8(1));
    s.put(1, DatumFactory.createInt8(1));
    s.put(2, DatumFactory.createInt8(1));
    VTuple e = new VTuple(3);
    e.put(0, DatumFactory.createInt8(4)); // 4
    e.put(1, DatumFactory.createInt8(2)); // 2
    e.put(2, DatumFactory.createInt8(3)); //x3 = 24

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    assertEquals(24, partitioner.getTotalCardinality().longValue());

    Tuple beforeOverflow = partitioner.increment(s, BigInteger.valueOf(5), 2);
    assertEquals(1, beforeOverflow.getInt8(0));
    assertEquals(2, beforeOverflow.getInt8(1));
    assertEquals(3, beforeOverflow.getInt8(2));
    Tuple overflow = partitioner.increment(beforeOverflow, BigInteger.valueOf(1), 2);
    assertEquals(2, overflow.getInt8(0));
    assertEquals(1, overflow.getInt8(1));
    assertEquals(1, overflow.getInt8(2));
  }

  @Test
  public void testIncrementOfFloat8() {
    Schema schema = new Schema()
        .addColumn("l_orderkey", Type.FLOAT8)
        .addColumn("l_linenumber", Type.FLOAT8)
        .addColumn("final", Type.FLOAT8);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(3);
    s.put(0, DatumFactory.createFloat8(1.1d));
    s.put(1, DatumFactory.createFloat8(1.1d));
    s.put(2, DatumFactory.createFloat8(1.1d));
    VTuple e = new VTuple(3);
    e.put(0, DatumFactory.createFloat8(4.1d)); // 4
    e.put(1, DatumFactory.createFloat8(2.1d)); // 2
    e.put(2, DatumFactory.createFloat8(3.1d)); //x3 = 24

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    assertEquals(24, partitioner.getTotalCardinality().longValue());

    Tuple beforeOverflow = partitioner.increment(s, BigInteger.valueOf(5), 2);
    assertTrue(1.1d == beforeOverflow.getFloat8(0));
    assertTrue(2.1d == beforeOverflow.getFloat8(1));
    assertTrue(3.1d == beforeOverflow.getFloat8(2));
    Tuple overflow = partitioner.increment(beforeOverflow, BigInteger.valueOf(1), 2);
    assertTrue(2.1d == overflow.getFloat8(0));
    assertTrue(1.1d == overflow.getFloat8(1));
    assertTrue(1.1d == overflow.getFloat8(2));
  }

  @Test
  public void testIncrementOfInet4() {
    Schema schema = new Schema()
        .addColumn("l_orderkey", Type.INET4)
        .addColumn("l_linenumber", Type.INET4)
        .addColumn("final", Type.INET4);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(3);
    s.put(0, DatumFactory.createInet4("127.0.1.1"));
    s.put(1, DatumFactory.createInet4("127.0.0.1"));
    s.put(2, DatumFactory.createInet4("128.0.0.253"));
    VTuple e = new VTuple(3);
    e.put(0, DatumFactory.createInet4("127.0.1.4")); // 4
    e.put(1, DatumFactory.createInet4("127.0.0.2")); // 2
    e.put(2, DatumFactory.createInet4("128.0.0.255")); //x3 = 24

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    assertEquals(24, partitioner.getTotalCardinality().longValue());

    Tuple beforeOverflow = partitioner.increment(s, BigInteger.valueOf(5), 2);
    assertTrue("127.0.1.1".equals(beforeOverflow.getText(0)));
    assertTrue("127.0.0.2".equals(beforeOverflow.getText(1)));
    assertTrue("128.0.0.255".equals(beforeOverflow.getText(2)));
    Tuple overflow = partitioner.increment(beforeOverflow, BigInteger.valueOf(1), 2);
    assertTrue("127.0.1.2".equals(overflow.getText(0)));
    assertTrue("127.0.0.1".equals(overflow.getText(1)));
    assertTrue("128.0.0.253".equals(overflow.getText(2)));
  }

  @Test
  public void testPartition() {
    Schema schema = new Schema();
    schema.addColumn("l_returnflag", Type.TEXT);
    schema.addColumn("l_linestatus", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(2);
    s.put(0, DatumFactory.createText("A"));
    s.put(1, DatumFactory.createText("F"));
    VTuple e = new VTuple(2);
    e.put(0, DatumFactory.createText("R"));
    e.put(1, DatumFactory.createText("O"));
    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner
        = new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(31);


    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
  }

  @Test
  public void testPartitionForOnePartNum() {
    Schema schema = new Schema()
        .addColumn("l_returnflag", Type.TEXT)
        .addColumn("l_linestatus", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(2);
    s.put(0, DatumFactory.createText("A"));
    s.put(1, DatumFactory.createText("F"));
    VTuple e = new VTuple(2);
    e.put(0, DatumFactory.createText("R"));
    e.put(1, DatumFactory.createText("O"));
    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner =
        new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(1);

    assertEquals(expected, ranges[0]);
  }

  @Test
  public void testPartitionForOnePartNumWithOneOfTheValueNull() {
    Schema schema = new Schema()
        .addColumn("l_returnflag", Type.TEXT)
        .addColumn("l_linestatus", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(2);
    s.put(0, DatumFactory.createNullDatum());
    s.put(1, DatumFactory.createText("F"));
    VTuple e = new VTuple(2);
    e.put(0, DatumFactory.createText("R"));
    e.put(1, DatumFactory.createNullDatum());
    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner =
        new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(1);

    assertEquals(expected, ranges[0]);
  }

  @Test
  public void testPartitionForMultipleChars() {
    Schema schema = new Schema()
        .addColumn("KEY1", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(1);
    s.put(0, DatumFactory.createText("AAA"));
    VTuple e = new VTuple(1);
    e.put(0, DatumFactory.createText("ZZZ"));

    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner =
        new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(48);

    TupleRange prev = null;
    for (int i = 0; i < ranges.length; i++) {
      if (prev != null) {
        assertTrue(i + "th, prev=" + prev + ",cur=" + ranges[i], prev.compareTo(ranges[i]) < 0);
      }
      prev = ranges[i];
    }
    assertEquals(48, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[47].getEnd().equals(e));
  }

  @Test
  public void testPartitionForMultipleChars2() {
    Schema schema = new Schema()
        .addColumn("KEY1", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(1);
    s.put(0, DatumFactory.createText("A1"));
    VTuple e = new VTuple(1);
    e.put(0, DatumFactory.createText("A999975"));

    final int partNum = 2;

    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner =
        new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForMultipleChars2Desc() {
    Schema schema = new Schema()
        .addColumn("KEY1", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);
    sortSpecs[0].setDescOrder();

    VTuple s = new VTuple(1);
    s.put(0, DatumFactory.createText("A999975"));
    VTuple e = new VTuple(1);
    e.put(0, DatumFactory.createText("A1"));

    final int partNum = 48;

    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner =
        new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForMultipleCharsWithSameFirstChar() {
    Schema schema = new Schema()
        .addColumn("KEY1", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(1);
    s.put(0, DatumFactory.createText("AAA"));
    VTuple e = new VTuple(1);
    e.put(0, DatumFactory.createText("AAZ"));

    final int partNum = 4;

    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner =
        new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
    assertEquals(partNum, ranges.length);
    assertTrue(ranges[0].getStart().equals(s));
    assertTrue(ranges[partNum - 1].getEnd().equals(e));
  }

  @Test
  public void testPartitionForOnePartNumWithBothValueNull() {
    Schema schema = new Schema()
        .addColumn("l_returnflag", Type.TEXT)
        .addColumn("l_linestatus", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(2);
    s.put(0, DatumFactory.createNullDatum());
    s.put(1, DatumFactory.createNullDatum());
    VTuple e = new VTuple(2);
    e.put(0, DatumFactory.createNullDatum());
    e.put(1, DatumFactory.createNullDatum());
    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner =
        new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(1);

    assertEquals(expected, ranges[0]);
  }

  @Test
  public void testPartitionWithNull() {
    Schema schema = new Schema();
    schema.addColumn("l_returnflag", Type.TEXT);
    schema.addColumn("l_linestatus", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(2);
    s.put(0, DatumFactory.createNullDatum());
    s.put(1, DatumFactory.createText("F"));
    VTuple e = new VTuple(2);
    e.put(0, DatumFactory.createNullDatum());
    e.put(1, DatumFactory.createText("O"));
    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner
        = new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(10);


    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
  }

  @Test
  public void testPartitionWithINET4() {
    Schema schema = new Schema();
    schema.addColumn("l_returnflag", Type.INET4);
    schema.addColumn("l_linestatus", Type.INET4);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    VTuple s = new VTuple(2);
    s.put(0, DatumFactory.createInet4("127.0.1.10"));
    s.put(1, DatumFactory.createInet4("127.0.2.10"));
    VTuple e = new VTuple(2);
    e.put(0, DatumFactory.createInet4("127.0.1.20"));
    e.put(1, DatumFactory.createInet4("127.0.2.20"));
    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner
        = new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(10);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev != null) {
        assertTrue(prev.compareTo(r) < 0);
      }
      prev = r;
    }
  }
}
