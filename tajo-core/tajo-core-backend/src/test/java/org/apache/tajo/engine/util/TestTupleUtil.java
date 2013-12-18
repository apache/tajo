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

package org.apache.tajo.engine.util;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.RangePartitionAlgorithm;
import org.apache.tajo.engine.planner.UniformRangePartition;
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.storage.*;
import org.apache.tajo.worker.dataserver.HttpUtil;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import static org.junit.Assert.*;

public class TestTupleUtil {
  @Test
  public final void testToBytesAndToTuple() {
    Schema schema = new Schema();
    schema.addColumn("col1", Type.BOOLEAN);
    schema.addColumn("col2", Type.BIT);
    schema.addColumn("col3", Type.CHAR);
    schema.addColumn("col4", Type.INT2);
    schema.addColumn("col5", Type.INT4);
    schema.addColumn("col6", Type.INT8);
    schema.addColumn("col7", Type.FLOAT4);
    schema.addColumn("col8", Type.FLOAT8);
    schema.addColumn("col9", Type.TEXT);
    schema.addColumn("col10", Type.BLOB);
    schema.addColumn("col11", Type.INET4);
    //schema.addColumn("col11", DataType.IPv6);
    
    Tuple tuple = new VTuple(11);
    tuple.put(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createBit((byte) 0x99),
        DatumFactory.createChar('7'),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23l),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("hyunsik"),
        DatumFactory.createBlob("hyunsik".getBytes()),
        DatumFactory.createInet4("192.168.0.1")
    });
    
    byte [] bytes = RowStoreUtil.RowStoreEncoder.toBytes(schema, tuple);
    Tuple tuple2 = RowStoreUtil.RowStoreDecoder.toTuple(schema, bytes);
    
    assertEquals(tuple, tuple2);
  }

  @Test
  public final void testGetPartitions() {
    Tuple sTuple = new VTuple(7);
    Tuple eTuple = new VTuple(7);

    Schema schema = new Schema();
    schema.addColumn("numByte", Type.BIT);
    schema.addColumn("numChar", Type.CHAR);
    schema.addColumn("numShort", Type.INT2);
    schema.addColumn("numInt", Type.INT4);
    schema.addColumn("numLong", Type.INT8);
    schema.addColumn("numFloat", Type.FLOAT4);
    schema.addColumn("numDouble", Type.FLOAT4);

    sTuple.put(0, DatumFactory.createBit((byte) 44));
    sTuple.put(1, DatumFactory.createChar('a'));
    sTuple.put(2, DatumFactory.createInt2((short) 10));
    sTuple.put(3, DatumFactory.createInt4(5));
    sTuple.put(4, DatumFactory.createInt8(100));
    sTuple.put(5, DatumFactory.createFloat4(100));
    sTuple.put(6, DatumFactory.createFloat8(100));

    eTuple.put(0, DatumFactory.createBit((byte) 99));
    eTuple.put(1, DatumFactory.createChar('p'));
    eTuple.put(2, DatumFactory.createInt2((short) 70));
    eTuple.put(3, DatumFactory.createInt4(70));
    eTuple.put(4, DatumFactory.createInt8(10000));
    eTuple.put(5, DatumFactory.createFloat4(150));
    eTuple.put(6, DatumFactory.createFloat8(170));

    RangePartitionAlgorithm partitioner = new UniformRangePartition(schema, new TupleRange(schema, sTuple, eTuple));
    TupleRange [] ranges = partitioner.partition(5);
    assertTrue(5 <= ranges.length);
    TupleComparator comp = new TupleComparator(schema, PlannerUtil.schemaToSortSpecs(schema));
    TupleRange prev = ranges[0];
    for (int i = 1; i < ranges.length; i++) {
      assertTrue(comp.compare(prev.getStart(), ranges[i].getStart()) < 0);
      assertTrue(comp.compare(prev.getEnd(), ranges[i].getEnd()) < 0);
      prev = ranges[i];
    }
  }

  @Test
  public void testQueryToRange() throws UnsupportedEncodingException {
    Schema schema = new Schema();
    schema.addColumn("intval", Type.INT4);
    schema.addColumn("floatval", Type.FLOAT4);

    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createInt4(5));
    s.put(1, DatumFactory.createFloat4(10));

    Tuple e = new VTuple(2);
    e.put(0, DatumFactory.createInt4(10));
    e.put(1, DatumFactory.createFloat4(20));

    TupleRange expected = new TupleRange(schema, s, e);
    int card = (int) TupleUtil.computeCardinality(schema, expected);
    assertEquals(66, card);
    int partNum = TupleUtil.getPartitions(schema, 5, expected).length;
    assertEquals(5, partNum);

    // TODO - partition should be improved to consider all fields
    //partNum = TupleUtil.partition(schema, 10, expected).length;
    //assertEquals(10, partNum);

    String query = TupleUtil.rangeToQuery(schema, expected, true, false);

    TupleRange range = TupleUtil.queryToRange(schema, query);
    assertEquals(expected, range);

    query = TupleUtil.rangeToQuery(schema, expected, true, true);
    Map<String,String> params = HttpUtil.getParamsFromQuery(query);
    assertTrue(params.containsKey("final"));
    range = TupleUtil.queryToRange(schema, query);
    assertEquals(expected, range);
  }

  @Test
  public void testQueryToRangeWithOneRange() throws UnsupportedEncodingException {
    Schema schema = new Schema();
    schema.addColumn("partkey", Type.FLOAT4);

    Tuple s = new VTuple(1);
    s.put(0, DatumFactory.createFloat4(28082));
    Tuple e = new VTuple(1);
    e.put(0, DatumFactory.createFloat4(28082));

    TupleRange expected = new TupleRange(schema, s, e);
    int card = (int) TupleUtil.computeCardinality(schema, expected);
    assertEquals(1, card);
    int partNum = TupleUtil.getPartitions(schema, card, expected).length;
    assertEquals(1, partNum);

    String query = TupleUtil.rangeToQuery(schema, expected, true, false);

    TupleRange range = TupleUtil.queryToRange(schema, query);
    assertEquals(expected, range);

    query = TupleUtil.rangeToQuery(schema, expected, true, true);
    Map<String,String> params = HttpUtil.getParamsFromQuery(query);
    assertTrue(params.containsKey("final"));
    range = TupleUtil.queryToRange(schema, query);
    assertEquals(expected, range);
  }

  @Test
  /**
   * It verifies NTA-805.
   */
  public void testRangeToQueryHeavyTest() throws UnsupportedEncodingException {
    Schema schema = new Schema();
    schema.addColumn("c_custkey", Type.INT4);
    Tuple s = new VTuple(1);
    s.put(0, DatumFactory.createInt4(4));
    Tuple e = new VTuple(1);
    e.put(0, DatumFactory.createInt4(149995));
    TupleRange expected = new TupleRange(schema, s, e);
    TupleRange [] ranges = TupleUtil.getPartitions(schema, 31, expected);

    String query;
    for (TupleRange range : ranges) {
      query = TupleUtil.rangeToQuery(schema, range, true, false);
      TupleRange result = TupleUtil.queryToRange(schema, query);
      assertEquals(range, result);
    }
  }

  @Test
  /**
   * It verifies NTA-807
   */
  public void testRangeToQueryTest() throws UnsupportedEncodingException {
    Schema schema = new Schema();
    schema.addColumn("l_returnflag", Type.TEXT);
    schema.addColumn("l_linestatus", Type.TEXT);
    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createText("A"));
    s.put(1, DatumFactory.createText("F"));
    Tuple e = new VTuple(2);
    e.put(0, DatumFactory.createText("R"));
    e.put(1, DatumFactory.createText("O"));
    TupleRange expected = new TupleRange(schema, s, e);

    RangePartitionAlgorithm partitioner = new UniformRangePartition(schema, expected, true);
    TupleRange [] ranges = partitioner.partition(31);

    String query;
    for (TupleRange range : ranges) {
      query = TupleUtil.rangeToQuery(schema, range, true, false);
      TupleRange result = TupleUtil.queryToRange(schema, query);
      assertEquals(range, result);
    }
  }

  @Test
  public void testBuildTupleFromPartitionPath() {

    Schema schema = new Schema();
    schema.addColumn("key1", Type.INT8);
    schema.addColumn("key2", Type.TEXT);

    Path path = new Path("hdfs://tajo/warehouse/partition_test/");
    Tuple tuple = TupleUtil.buildTupleFromPartitionPath(schema, path, true);
    assertNull(tuple);
    tuple = TupleUtil.buildTupleFromPartitionPath(schema, path, false);
    assertNull(tuple);

    path = new Path("hdfs://tajo/warehouse/partition_test/key1=123");
    tuple = TupleUtil.buildTupleFromPartitionPath(schema, path, true);
    assertNotNull(tuple);
    assertEquals(DatumFactory.createInt8(123), tuple.get(0));
    tuple = TupleUtil.buildTupleFromPartitionPath(schema, path, false);
    assertNotNull(tuple);
    assertEquals(DatumFactory.createInt8(123), tuple.get(0));

    path = new Path("hdfs://tajo/warehouse/partition_test/key1=123/part-0000"); // wrong cases;
    tuple = TupleUtil.buildTupleFromPartitionPath(schema, path, true);
    assertNull(tuple);
    tuple = TupleUtil.buildTupleFromPartitionPath(schema, path, false);
    assertNull(tuple);

    path = new Path("hdfs://tajo/warehouse/partition_test/key1=123/key2=abc");
    tuple = TupleUtil.buildTupleFromPartitionPath(schema, path, true);
    assertNotNull(tuple);
    assertEquals(DatumFactory.createInt8(123), tuple.get(0));
    assertEquals(DatumFactory.createText("abc"), tuple.get(1));
    tuple = TupleUtil.buildTupleFromPartitionPath(schema, path, false);
    assertNotNull(tuple);
    assertEquals(DatumFactory.createInt8(123), tuple.get(0));
    assertEquals(DatumFactory.createText("abc"), tuple.get(1));


    path = new Path("hdfs://tajo/warehouse/partition_test/key1=123/key2=abc/part-0001");
    tuple = TupleUtil.buildTupleFromPartitionPath(schema, path, true);
    assertNull(tuple);

    tuple = TupleUtil.buildTupleFromPartitionPath(schema, path, false);
    assertNotNull(tuple);
    assertEquals(DatumFactory.createInt8(123), tuple.get(0));
    assertEquals(DatumFactory.createText("abc"), tuple.get(1));
  }
}
