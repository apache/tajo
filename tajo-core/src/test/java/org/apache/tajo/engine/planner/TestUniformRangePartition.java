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
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.VTuple;
import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestUniformRangePartition {
  /**
   * It verify overflow and increment.
   */
  @Test
  public void testIncrement1() {
    Schema schema = new Schema()
        .addColumn("l_returnflag", Type.TEXT)
        .addColumn("l_linestatus", Type.TEXT);

    SortSpec[] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createText("A"));
    s.put(1, DatumFactory.createText("A"));
    Tuple e = new VTuple(2);
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
    assertEquals("A", end.get(0).asChars());
    assertEquals("B", end.get(1).asChars());
    for (int i = 2; i < 11; i++ ) {
      end = partitioner.increment(end, BigInteger.valueOf(1), 1);
      assertEquals(result[i].charAt(0), end.get(0).asChars().charAt(0));
      assertEquals(result[i].charAt(1), end.get(1).asChars().charAt(0));
    }
  }

  /**
   * It verify overflow with the number that exceeds the last digit.
   */
  @Test
  public void testIncrement2() {
    Schema schema = new Schema()
        .addColumn("l_returnflag", Type.TEXT)
        .addColumn("l_linestatus", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createText("A"));
    s.put(1, DatumFactory.createText("A"));
    Tuple e = new VTuple(2);
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
    assertEquals("C", end.get(0).asChars());
    assertEquals("A", end.get(1).asChars());
    end = partitioner.increment(end, BigInteger.valueOf(5), 1);
    assertEquals("D", end.get(0).asChars());
    assertEquals("C", end.get(1).asChars());
  }

  /**
   * It verify the case where two or more digits are overflow.
   */
  @Test
  public void testIncrement3() {
    Schema schema = new Schema()
        .addColumn("l_returnflag", Type.TEXT)
        .addColumn("l_linestatus", Type.TEXT)
        .addColumn("final", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    Tuple s = new VTuple(3);
    s.put(0, DatumFactory.createText("A"));
    s.put(1, DatumFactory.createText("A"));
    s.put(2, DatumFactory.createText("A"));
    Tuple e = new VTuple(3);
    e.put(0, DatumFactory.createText("D")); //  4
    e.put(1, DatumFactory.createText("B")); //  2
    e.put(2, DatumFactory.createText("C")); // x3 = 24

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    assertEquals(24, partitioner.getTotalCardinality().intValue());

    Tuple overflowBefore = partitioner.increment(s, BigInteger.valueOf(5), 2);
    assertEquals("A", overflowBefore.get(0).asChars());
    assertEquals("B", overflowBefore.get(1).asChars());
    assertEquals("C", overflowBefore.get(2).asChars());
    Tuple overflowed = partitioner.increment(overflowBefore, BigInteger.valueOf(1), 2);
    assertEquals("B", overflowed.get(0).asChars());
    assertEquals("A", overflowed.get(1).asChars());
    assertEquals("A", overflowed.get(2).asChars());
  }

  @Test
  public void testIncrement4() {
    Schema schema = new Schema()
        .addColumn("l_orderkey", Type.INT8)
        .addColumn("l_linenumber", Type.INT8);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createInt8(10));
    s.put(1, DatumFactory.createInt8(20));
    Tuple e = new VTuple(2);
    e.put(0, DatumFactory.createInt8(19));
    e.put(1, DatumFactory.createInt8(39));

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    assertEquals(200, partitioner.getTotalCardinality().longValue());

    Tuple range2 = partitioner.increment(s, BigInteger.valueOf(100), 1);
    assertEquals(15, range2.get(0).asInt4());
    assertEquals(20, range2.get(1).asInt4());
    Tuple range3 = partitioner.increment(range2, BigInteger.valueOf(99), 1);
    assertEquals(19, range3.get(0).asInt4());
    assertEquals(39, range3.get(1).asInt4());
  }

  @Test public void testIncrement5() {
    Schema schema = new Schema()
        .addColumn("l_orderkey", Type.INT8)
        .addColumn("l_linenumber", Type.INT8)
        .addColumn("final", Type.INT8);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    Tuple s = new VTuple(3);
    s.put(0, DatumFactory.createInt8(1));
    s.put(1, DatumFactory.createInt8(1));
    s.put(2, DatumFactory.createInt8(1));
    Tuple e = new VTuple(3);
    e.put(0, DatumFactory.createInt8(4)); // 4
    e.put(1, DatumFactory.createInt8(2)); // 2
    e.put(2, DatumFactory.createInt8(3)); //x3 = 24

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    assertEquals(24, partitioner.getTotalCardinality().longValue());

    Tuple beforeOverflow = partitioner.increment(s, BigInteger.valueOf(5), 2);
    assertEquals(1, beforeOverflow.get(0).asInt8());
    assertEquals(2, beforeOverflow.get(1).asInt8());
    assertEquals(3, beforeOverflow.get(2).asInt8());
    Tuple overflow = partitioner.increment(beforeOverflow, BigInteger.valueOf(1), 2);
    assertEquals(2, overflow.get(0).asInt8());
    assertEquals(1, overflow.get(1).asInt8());
    assertEquals(1, overflow.get(2).asInt8());
  }

  @Test
  public void testIncrement6() {
    Schema schema = new Schema()
        .addColumn("l_orderkey", Type.FLOAT8)
        .addColumn("l_linenumber", Type.FLOAT8)
        .addColumn("final", Type.FLOAT8);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    Tuple s = new VTuple(3);
    s.put(0, DatumFactory.createFloat8(1.1d));
    s.put(1, DatumFactory.createFloat8(1.1d));
    s.put(2, DatumFactory.createFloat8(1.1d));
    Tuple e = new VTuple(3);
    e.put(0, DatumFactory.createFloat8(4.1d)); // 4
    e.put(1, DatumFactory.createFloat8(2.1d)); // 2
    e.put(2, DatumFactory.createFloat8(3.1d)); //x3 = 24

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    assertEquals(24, partitioner.getTotalCardinality().longValue());

    Tuple beforeOverflow = partitioner.increment(s, BigInteger.valueOf(5), 2);
    assertTrue(1.1d == beforeOverflow.get(0).asFloat8());
    assertTrue(2.1d == beforeOverflow.get(1).asFloat8());
    assertTrue(3.1d == beforeOverflow.get(2).asFloat8());
    Tuple overflow = partitioner.increment(beforeOverflow, BigInteger.valueOf(1), 2);
    assertTrue(2.1d == overflow.get(0).asFloat8());
    assertTrue(1.1d == overflow.get(1).asFloat8());
    assertTrue(1.1d == overflow.get(2).asFloat8());
  }

  @Test
  public void testIncrement7() {
    Schema schema = new Schema()
        .addColumn("l_orderkey", Type.INET4)
        .addColumn("l_linenumber", Type.INET4)
        .addColumn("final", Type.INET4);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    Tuple s = new VTuple(3);
    s.put(0, DatumFactory.createInet4("127.0.1.1"));
    s.put(1, DatumFactory.createInet4("127.0.0.1"));
    s.put(2, DatumFactory.createInet4("128.0.0.253"));
    Tuple e = new VTuple(3);
    e.put(0, DatumFactory.createInet4("127.0.1.4")); // 4
    e.put(1, DatumFactory.createInet4("127.0.0.2")); // 2
    e.put(2, DatumFactory.createInet4("128.0.0.255")); //x3 = 24

    TupleRange expected = new TupleRange(sortSpecs, s, e);

    UniformRangePartition partitioner = new UniformRangePartition(expected, sortSpecs);
    assertEquals(24, partitioner.getTotalCardinality().longValue());

    Tuple beforeOverflow = partitioner.increment(s, BigInteger.valueOf(5), 2);
    assertTrue("127.0.1.1".equals(beforeOverflow.get(0).asChars()));
    assertTrue("127.0.0.2".equals(beforeOverflow.get(1).asChars()));
    assertTrue("128.0.0.255".equals(beforeOverflow.get(2).asChars()));
    Tuple overflow = partitioner.increment(beforeOverflow, BigInteger.valueOf(1), 2);
    assertTrue("127.0.1.2".equals(overflow.get(0).asChars()));
    assertTrue("127.0.0.1".equals(overflow.get(1).asChars()));
    assertTrue("128.0.0.253".equals(overflow.get(2).asChars()));
  }

  @Test
  public void testPartition() {
    Schema schema = new Schema();
    schema.addColumn("l_returnflag", Type.TEXT);
    schema.addColumn("l_linestatus", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createText("A"));
    s.put(1, DatumFactory.createText("F"));
    Tuple e = new VTuple(2);
    e.put(0, DatumFactory.createText("R"));
    e.put(1, DatumFactory.createText("O"));
    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner
        = new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(31);


    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev == null) {
        prev = r;
      } else {
        assertTrue(prev.compareTo(r) < 0);
      }
    }
  }

  @Test
  public void testPartitionForOnePartNum() {
    Schema schema = new Schema()
        .addColumn("l_returnflag", Type.TEXT)
        .addColumn("l_linestatus", Type.TEXT);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createText("A"));
    s.put(1, DatumFactory.createText("F"));
    Tuple e = new VTuple(2);
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

    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createNullDatum());
    s.put(1, DatumFactory.createText("F"));
    Tuple e = new VTuple(2);
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

    Tuple s = new VTuple(1);
    s.put(0, DatumFactory.createText("AAA"));
    Tuple e = new VTuple(1);
    e.put(0, DatumFactory.createText("ZZZ"));

    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner =
        new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(48);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev == null) {
        prev = r;
      } else {
        assertTrue(prev.compareTo(r) < 0);
      }
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

    Tuple s = new VTuple(1);
    s.put(0, DatumFactory.createText("A1"));
    Tuple e = new VTuple(1);
    e.put(0, DatumFactory.createText("A999975"));

    final int partNum = 2;

    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner =
        new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev == null) {
        prev = r;
      } else {
        assertTrue(prev.compareTo(r) < 0);
      }
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

    Tuple s = new VTuple(1);
    s.put(0, DatumFactory.createText("A999975"));
    Tuple e = new VTuple(1);
    e.put(0, DatumFactory.createText("A1"));

    final int partNum = 48;

    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner =
        new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev == null) {
        prev = r;
      } else {
        assertTrue(prev.compareTo(r) > 0);
      }
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

    Tuple s = new VTuple(1);
    s.put(0, DatumFactory.createText("AAA"));
    Tuple e = new VTuple(1);
    e.put(0, DatumFactory.createText("AAZ"));

    final int partNum = 4;

    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner =
        new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(partNum);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev == null) {
        prev = r;
      } else {
        assertTrue(prev.compareTo(r) < 0);
      }
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

    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createNullDatum());
    s.put(1, DatumFactory.createNullDatum());
    Tuple e = new VTuple(2);
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

    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createNullDatum());
    s.put(1, DatumFactory.createText("F"));
    Tuple e = new VTuple(2);
    e.put(0, DatumFactory.createNullDatum());
    e.put(1, DatumFactory.createText("O"));
    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner
        = new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(10);


    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev == null) {
        prev = r;
      } else {
        assertTrue(prev.compareTo(r) < 0);
      }
    }
  }

  @Test
  public void testPartitionWithINET4() {
    Schema schema = new Schema();
    schema.addColumn("l_returnflag", Type.INET4);
    schema.addColumn("l_linestatus", Type.INET4);

    SortSpec [] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createInet4("127.0.1.10"));
    s.put(1, DatumFactory.createInet4("127.0.2.10"));
    Tuple e = new VTuple(2);
    e.put(0, DatumFactory.createInet4("127.0.1.20"));
    e.put(1, DatumFactory.createInet4("127.0.2.20"));
    TupleRange expected = new TupleRange(sortSpecs, s, e);
    RangePartitionAlgorithm partitioner
        = new UniformRangePartition(expected, sortSpecs, true);
    TupleRange [] ranges = partitioner.partition(10);

    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev == null) {
        prev = r;
      } else {
        assertTrue(prev.compareTo(r) < 0);
      }
    }
  }
}
