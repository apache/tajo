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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.RangePartitionAlgorithm;
import org.apache.tajo.engine.planner.UniformRangePartition;
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.storage.*;
import org.junit.Test;

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

    SortSpec[] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

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

    RangePartitionAlgorithm partitioner = new UniformRangePartition(new TupleRange(sortSpecs, sTuple, eTuple),
        sortSpecs);
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
