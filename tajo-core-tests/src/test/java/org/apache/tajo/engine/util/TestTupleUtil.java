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
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.planner.RangePartitionAlgorithm;
import org.apache.tajo.engine.planner.UniformRangePartition;
import org.apache.tajo.plan.rewrite.rules.PartitionedTableRewriter;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.RowStoreUtil.RowStoreDecoder;
import org.apache.tajo.storage.RowStoreUtil.RowStoreEncoder;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestTupleUtil {
  @Test
  public final void testFixedSizeChar() {
    Schema schema = SchemaBuilder.builder().add("col1", CatalogUtil.newDataTypeWithLen(Type.CHAR, 5)).build();

    Tuple tuple = new VTuple(1);
    tuple.put(new Datum[] {
      DatumFactory.createChar("abc\0\0")
    });

    RowStoreEncoder encoder = RowStoreUtil.createEncoder(schema);
    RowStoreDecoder decoder = RowStoreUtil.createDecoder(schema);
    byte [] bytes = encoder.toBytes(tuple);
    Tuple tuple2 = decoder.toTuple(bytes);

    assertEquals(tuple, tuple2);
  }

  @Test
  public final void testToBytesAndToTuple() {
    Schema schema = SchemaBuilder.builder()
        .add("col1", Type.BOOLEAN)
        .add("col2", Type.CHAR)
        .add("col3", Type.INT2)
        .add("col4", Type.INT4)
        .add("col5", Type.INT8)
        .add("col6", Type.FLOAT4)
        .add("col7", Type.FLOAT8)
        .add("col8", Type.TEXT)
        .add("col9", Type.BLOB)
        .build();
    //schema.addColumn("col11", DataType.IPv6);
    
    Tuple tuple = new VTuple(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createChar('7'),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23l),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("hyunsik"),
        DatumFactory.createBlob("hyunsik".getBytes()),
    });

    RowStoreEncoder encoder = RowStoreUtil.createEncoder(schema);
    RowStoreDecoder decoder = RowStoreUtil.createDecoder(schema);
    byte [] bytes = encoder.toBytes(tuple);
    Tuple tuple2 = decoder.toTuple(bytes);
    
    assertEquals(tuple, tuple2);
  }

  @Test
  public final void testGetPartitions() {
    VTuple sTuple = new VTuple(6);
    VTuple eTuple = new VTuple(6);

    Schema schema = SchemaBuilder.builder()
        .add("numChar", Type.CHAR)
        .add("numShort", Type.INT2)
        .add("numInt", Type.INT4)
        .add("numLong", Type.INT8)
        .add("numFloat", Type.FLOAT4)
        .add("numDouble", Type.FLOAT4)
        .build();

    SortSpec[] sortSpecs = PlannerUtil.schemaToSortSpecs(schema);

    sTuple.put(0, DatumFactory.createChar('a'));
    sTuple.put(1, DatumFactory.createInt2((short) 10));
    sTuple.put(2, DatumFactory.createInt4(5));
    sTuple.put(3, DatumFactory.createInt8(100));
    sTuple.put(4, DatumFactory.createFloat4(100));
    sTuple.put(5, DatumFactory.createFloat8(100));

    eTuple.put(0, DatumFactory.createChar('p'));
    eTuple.put(1, DatumFactory.createInt2((short) 70));
    eTuple.put(2, DatumFactory.createInt4(70));
    eTuple.put(3, DatumFactory.createInt8(10000));
    eTuple.put(4, DatumFactory.createFloat4(150));
    eTuple.put(5, DatumFactory.createFloat8(170));

    RangePartitionAlgorithm partitioner = new UniformRangePartition(new TupleRange(sortSpecs, sTuple, eTuple),
        sortSpecs);
    TupleRange [] ranges = partitioner.partition(4);
    assertTrue(4 <= ranges.length);
    BaseTupleComparator comp = new BaseTupleComparator(schema, PlannerUtil.schemaToSortSpecs(schema));
    TupleRange prev = ranges[0];
    for (int i = 1; i < ranges.length; i++) {
      assertTrue(comp.compare(prev.getStart(), ranges[i].getStart()) < 0);
      assertTrue(comp.compare(prev.getEnd(), ranges[i].getEnd()) < 0);
      prev = ranges[i];
    }
  }

  @Test
  public void testBuildTupleFromPartitionPath() {

    Schema schema = SchemaBuilder.builder()
        .add("key1", Type.INT8)
        .add("key2", Type.TEXT)
        .build();

    Path path = new Path("hdfs://tajo/warehouse/partition_test/");
    Tuple tuple = PartitionedTableRewriter.buildTupleFromPartitionPath(schema, path, true);
    assertNull(tuple);
    tuple = PartitionedTableRewriter.buildTupleFromPartitionPath(schema, path, false);
    assertNull(tuple);

    path = new Path("hdfs://tajo/warehouse/partition_test/key1=123");
    tuple = PartitionedTableRewriter.buildTupleFromPartitionPath(schema, path, true);
    assertNotNull(tuple);
    assertEquals(DatumFactory.createInt8(123), tuple.asDatum(0));
    tuple = PartitionedTableRewriter.buildTupleFromPartitionPath(schema, path, false);
    assertNotNull(tuple);
    assertEquals(DatumFactory.createInt8(123), tuple.asDatum(0));

    path = new Path("hdfs://tajo/warehouse/partition_test/key1=123/part-0000"); // wrong cases;
    tuple = PartitionedTableRewriter.buildTupleFromPartitionPath(schema, path, true);
    assertNull(tuple);
    tuple = PartitionedTableRewriter.buildTupleFromPartitionPath(schema, path, false);
    assertNull(tuple);

    path = new Path("hdfs://tajo/warehouse/partition_test/key1=123/key2=abc");
    tuple = PartitionedTableRewriter.buildTupleFromPartitionPath(schema, path, true);
    assertNotNull(tuple);
    assertEquals(DatumFactory.createInt8(123), tuple.asDatum(0));
    assertEquals(DatumFactory.createText("abc"), tuple.asDatum(1));
    tuple = PartitionedTableRewriter.buildTupleFromPartitionPath(schema, path, false);
    assertNotNull(tuple);
    assertEquals(DatumFactory.createInt8(123), tuple.asDatum(0));
    assertEquals(DatumFactory.createText("abc"), tuple.asDatum(1));


    path = new Path("hdfs://tajo/warehouse/partition_test/key1=123/key2=abc/part-0001");
    tuple = PartitionedTableRewriter.buildTupleFromPartitionPath(schema, path, true);
    assertNull(tuple);

    tuple = PartitionedTableRewriter.buildTupleFromPartitionPath(schema, path, false);
    assertNotNull(tuple);
    assertEquals(DatumFactory.createInt8(123), tuple.asDatum(0));
    assertEquals(DatumFactory.createText("abc"), tuple.asDatum(1));
  }

  @Test
  public void testBuildTupleFromPartitionName() {
    Schema schema = SchemaBuilder.builder()
      .add("key1", Type.INT8)
      .add("key2", Type.TEXT)
      .build();

    Tuple tuple = PartitionedTableRewriter.buildTupleFromPartitionKeys(schema, "key1=123");
    assertNotNull(tuple);
    assertEquals(DatumFactory.createInt8(123), tuple.asDatum(0));
    assertEquals(DatumFactory.createNullDatum(), tuple.asDatum(1));

    tuple = PartitionedTableRewriter.buildTupleFromPartitionKeys(schema, "key1=123/key2=abc");
    assertNotNull(tuple);
    assertEquals(DatumFactory.createInt8(123), tuple.asDatum(0));
    assertEquals(DatumFactory.createText("abc"), tuple.asDatum(1));

    tuple = PartitionedTableRewriter.buildTupleFromPartitionKeys(schema, "key2=abc");
    assertNotNull(tuple);
    assertEquals(DatumFactory.createNullDatum(), tuple.asDatum(0));
    assertEquals(DatumFactory.createText("abc"), tuple.asDatum(1));

  }
}
