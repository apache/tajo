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

package org.apache.tajo.tuple.memory;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.exception.ValueOutOfRangeException;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.tuple.RowBlockReader;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.NumberUtil;
import org.apache.tajo.util.ProtoUtil;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.tajo.common.TajoDataTypes.Type;
import static org.junit.Assert.*;

public class TestMemoryRowBlock {
  private static final Log LOG = LogFactory.getLog(TestMemoryRowBlock.class);
  public static String UNICODE_FIELD_PREFIX = "abc_가나다_";
  public static DataType[] schema;

  static {
    schema = new DataType[] {
        DataType.newBuilder().setType(Type.BOOLEAN).build(),
        DataType.newBuilder().setType(Type.INT2).build(),
        DataType.newBuilder().setType(Type.INT4).build(),
        DataType.newBuilder().setType(Type.INT8).build(),
        DataType.newBuilder().setType(Type.FLOAT4).build(),
        DataType.newBuilder().setType(Type.FLOAT8).build(),
        DataType.newBuilder().setType(Type.TEXT).build(),
        DataType.newBuilder().setType(Type.TIMESTAMP).build(),
        DataType.newBuilder().setType(Type.DATE).build(),
        DataType.newBuilder().setType(Type.TIME).build(),
        DataType.newBuilder().setType(Type.INTERVAL).build(),
        DataType.newBuilder().setType(Type.INET4).build(),
        DataType.newBuilder().setType(Type.PROTOBUF).setCode(PrimitiveProtos.StringProto.class.getName()).build()
    };
  }

  private void explainRowBlockAllocation(MemoryRowBlock rowBlock, long startTime, long endTime) {
    LOG.info(FileUtil.humanReadableByteCount(rowBlock.capacity(), true) + " bytes allocated "
        + (endTime - startTime) + " msec");
  }

  @Test
  public void testPutAndReadValidation() {
    int rowNum = 1000;

    long allocStart = System.currentTimeMillis();
    MemoryRowBlock rowBlock = new MemoryRowBlock(schema, 1024);
    long allocEnd = System.currentTimeMillis();
    explainRowBlockAllocation(rowBlock, allocStart, allocEnd);

    RowBlockReader reader = null;

    ZeroCopyTuple tuple = new UnSafeTuple();
    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < rowNum; i++) {
      fillRow(i, rowBlock.getWriter());

      reader = rowBlock.getReader();
      int j = 0;
      while(reader.next(tuple)) {
        validateTupleResult(j, tuple);
        j++;
      }
    }

    assertNotNull(reader);
    long writeEnd = System.currentTimeMillis();
    LOG.info("writing and validating take " + (writeEnd - writeStart) + " msec");

    long readStart = System.currentTimeMillis();
    tuple = new UnSafeTuple();
    int j = 0;
    reader.reset();
    while(reader.next(tuple)) {
      validateTupleResult(j, tuple);
      j++;
    }
    assertEquals(rowNum, j);
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");

    rowBlock.release();
  }

  @Test
  public void testPutAndCancelValidation() {
    VTuple vTuple = new VTuple(schema.length);
    fillVTuple(0, vTuple);

    //get memory size of 1 row
    MemoryRowBlock rowBlock = new MemoryRowBlock(schema);
    fillRow(0, rowBlock.getWriter());
    int rowSize = rowBlock.usedMem();
    rowBlock.release();

    rowBlock = new MemoryRowBlock(schema, new FixedSizeLimitSpec(rowSize / 2, 0.0f), true);
    assertFalse(rowBlock.getWriter().addTuple(vTuple));
    try {
      OffHeapRowBlockUtils.convert(vTuple, rowBlock.getWriter());
      fail();
    } catch (Exception e) {
      assertEquals(ValueOutOfRangeException.class, e.getClass());
    }
    rowBlock.release();

    //allow 1 row
    rowBlock = new MemoryRowBlock(schema, new FixedSizeLimitSpec(rowSize, 0.0f), true);
    assertTrue(rowBlock.getWriter().addTuple(vTuple));
    assertFalse(rowBlock.getWriter().addTuple(vTuple));
    assertEquals(1, rowBlock.rows());

    ZeroCopyTuple tuple = new UnSafeTuple();
    RowBlockReader reader = rowBlock.getReader();
    assertTrue(reader.next(tuple));
    validateTupleResult(0, tuple);
    assertFalse(reader.next(tuple));
    rowBlock.release();
  }

  @Test
  public void testNullityValidation() {
    int rowNum = 1000;

    long allocStart = System.currentTimeMillis();
    MemoryRowBlock rowBlock = new MemoryRowBlock(schema, 1024);
    long allocEnd = System.currentTimeMillis();
    explainRowBlockAllocation(rowBlock, allocStart, allocEnd);

    RowBlockReader reader = null;
    ZeroCopyTuple tuple = new UnSafeTuple();
    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < rowNum; i++) {

      fillRowBlockWithNull(i, rowBlock.getWriter());

      reader = rowBlock.getReader();
      int j = 0;
      while(reader.next(tuple)) {
        validateNullity(j, tuple);

        j++;
      }
    }

    assertNotNull(reader);
    long writeEnd = System.currentTimeMillis();
    LOG.info("writing and nullity validating take " + (writeEnd - writeStart) + " msec");

    long readStart = System.currentTimeMillis();
    tuple = new UnSafeTuple();
    int j = 0;
    reader.reset();
    while(reader.next(tuple)) {
      validateNullity(j, tuple);

      j++;
    }
    assertEquals(rowNum, j);
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");

    rowBlock.release();
  }

  @Test
  public void testEmptyRow() {
    int rowNum = 1000;

    long allocStart = System.currentTimeMillis();
    MemoryRowBlock rowBlock = new MemoryRowBlock(schema, StorageUnit.MB * 10);
    long allocEnd = System.currentTimeMillis();
    explainRowBlockAllocation(rowBlock, allocStart, allocEnd);

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < rowNum; i++) {
      rowBlock.getWriter().startRow();
      // empty columns
      rowBlock.getWriter().endRow();
    }
    long writeEnd = System.currentTimeMillis();
    LOG.info("writing tooks " + (writeEnd - writeStart) + " msec");

    RowBlockReader reader = rowBlock.getReader();

    long readStart = System.currentTimeMillis();
    ZeroCopyTuple tuple = new UnSafeTuple();
    int j = 0;
    reader.reset();
    while(reader.next(tuple)) {
      j++;
    }

    assertEquals(rowNum, j);
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");
    rowBlock.release();

    assertEquals(rowNum, j);
    assertEquals(rowNum, rowBlock.rows());
  }

  @Test
  public void testSortBenchmark() {
    int rowNum = 1000;

    MemoryRowBlock rowBlock = createRowBlock(rowNum);
    List<ZeroCopyTuple> unSafeTuples = Lists.newArrayList();

    long readStart = System.currentTimeMillis();
    ZeroCopyTuple tuple = new UnSafeTuple();

    RowBlockReader reader = rowBlock.getReader();
    while(reader.next(tuple)) {
      unSafeTuples.add(tuple);
      tuple = new UnSafeTuple();
    }
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");

    long sortStart = System.currentTimeMillis();
    Collections.sort(unSafeTuples, new Comparator<ZeroCopyTuple>() {
      @Override
      public int compare(ZeroCopyTuple t1, ZeroCopyTuple t2) {
        return NumberUtil.compare(t1.getInt4(2), t2.getInt4(2));
      }
    });
    long sortEnd = System.currentTimeMillis();
    LOG.info("sorting took " + (sortEnd - sortStart) + " msec");
    rowBlock.release();
  }

  @Test
  public void testVTuplePutAndGetBenchmark() {
    int rowNum = 1000;

    List<VTuple> rowBlock = Lists.newArrayList();
    long writeStart = System.currentTimeMillis();
    VTuple tuple;
    for (int i = 0; i < rowNum; i++) {
      tuple = new VTuple(schema.length);
      fillVTuple(i, tuple);
      rowBlock.add(tuple);
    }
    long writeEnd = System.currentTimeMillis();
    LOG.info("Writing takes " + (writeEnd - writeStart) + " msec");

    long readStart = System.currentTimeMillis();
    int j = 0;
    for (VTuple t : rowBlock) {
      validateTupleResult(j, t);
      j++;
    }

    assertEquals(rowNum, j);
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");

    int count = 0;
    for (VTuple aRowBlock : rowBlock) {
      for (int m = 0; m < schema.length; m++) {
        if (aRowBlock.contains(m) && aRowBlock.get(m).type() == Type.INT4) {
          count++;
        }
      }
    }
    // For preventing unnecessary code elimination optimization.
    LOG.info("The number of INT4 values is " + count + ".");
  }

  @Test
  public void testVTuplePutAndGetBenchmarkViaDirectRowEncoder() {
    int rowNum = 1000;

    MemoryRowBlock rowBlock = new MemoryRowBlock(schema, StorageUnit.MB * 100);

    long writeStart = System.currentTimeMillis();
    VTuple tuple = new VTuple(schema.length);
    for (int i = 0; i < rowNum; i++) {
      fillVTuple(i, tuple);
      rowBlock.getWriter().addTuple(tuple);
    }
    long writeEnd = System.currentTimeMillis();
    LOG.info("Writing takes " + (writeEnd - writeStart) + " msec");

    validateResults(rowBlock);
    rowBlock.release();
  }

  @Test
  public void testSerDerOfRowBlock() {
    int rowNum = 1000;

    MemoryRowBlock rowBlock = createRowBlock(rowNum);

    MemoryRowBlock restoredRowBlock = new MemoryRowBlock(rowBlock);
    validateResults(restoredRowBlock);
    rowBlock.release();
  }

  @Test
  public void testSerDerOfZeroCopyTuple() {
    int rowNum = 1000;

    MemoryRowBlock rowBlock = createRowBlock(rowNum);

    MemoryRowBlock restoredRowBlock = new MemoryRowBlock(rowBlock);
    RowBlockReader reader = restoredRowBlock.getReader();

    long readStart = System.currentTimeMillis();
    UnSafeTuple tuple = new UnSafeTuple();

    int j = 0;
    List<ZeroCopyTuple> copyTuples = Lists.newArrayList();

    while (reader.next(tuple)) {
      validateTupleResult(j, tuple);

      UnSafeTuple copyTuple = new UnSafeTuple();
      copyTuple.set(tuple);
      copyTuples.add(copyTuple);

      j++;
    }

    assertEquals(rowNum, j);

    for (int i = 0; i < j; i++) {
      validateTupleResult(i, copyTuples.get(i));
    }

    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");

    rowBlock.release();
  }

  public static MemoryRowBlock createRowBlock(int rowNum) {
    long allocateStart = System.currentTimeMillis();
    MemoryRowBlock rowBlock = new MemoryRowBlock(schema, StorageUnit.MB * 8);
    long allocatedEnd = System.currentTimeMillis();
    LOG.info(FileUtil.humanReadableByteCount(rowBlock.capacity(), true) + " bytes allocated "
        + (allocatedEnd - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < rowNum; i++) {
      fillRow(i, rowBlock.getWriter());
    }
    long writeEnd = System.currentTimeMillis();
    LOG.info("writing takes " + (writeEnd - writeStart) + " msec");

    return rowBlock;
  }

  public static MemoryRowBlock createRowBlockWithNull(int rowNum) {
    long allocateStart = System.currentTimeMillis();
    MemoryRowBlock rowBlock = new MemoryRowBlock(schema, StorageUnit.MB * 8);
    long allocatedEnd = System.currentTimeMillis();
    LOG.info(FileUtil.humanReadableByteCount(rowBlock.capacity(), true) + " bytes allocated "
        + (allocatedEnd - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < rowNum; i++) {
      fillRowBlockWithNull(i, rowBlock.getWriter());
    }
    long writeEnd = System.currentTimeMillis();
    LOG.info("writing and validating take " + (writeEnd - writeStart) + " msec");

    return rowBlock;
  }

  public static void fillRow(int i, RowWriter builder) {
    builder.startRow();
    builder.putBool(i % 1 == 0 ? true : false); // 0
    builder.putInt2((short) 1);                 // 1
    builder.putInt4(i);                         // 2
    builder.putInt8(i);                         // 3
    builder.putFloat4(i);                       // 4
    builder.putFloat8(i);                       // 5
    builder.putText(UNICODE_FIELD_PREFIX + i);  // 6
    builder.putTimestamp(DatumFactory.createTimestamp("2014-04-16 08:48:00").asInt8() + i); // 7
    builder.putDate(DatumFactory.createDate("2014-04-16").asInt4() + i); // 8
    builder.putTime(DatumFactory.createTime("08:48:00").asInt8() + i); // 9
    builder.putInterval(DatumFactory.createInterval((i + 1) + " hours")); // 10
    builder.putInet4(DatumFactory.createInet4("192.168.0.1").asInt4() + i); // 11
    builder.putProtoDatum(new ProtobufDatum(ProtoUtil.convertString(i + ""))); // 12
    builder.endRow();
  }

  public static void fillRowBlockWithNull(int i, RowWriter writer) {
    writer.startRow();

    if (i == 0) {
      writer.skipField();
    } else {
      writer.putBool(i % 1 == 0 ? true : false); // 0
    }
    if (i % 1 == 0) {
      writer.skipField();
    } else {
      writer.putInt2((short) 1);                 // 1
    }

    if (i % 2 == 0) {
      writer.skipField();
    } else {
      writer.putInt4(i);                         // 2
    }

    if (i % 3 == 0) {
      writer.skipField();
    } else {
      writer.putInt8(i);                         // 3
    }

    if (i % 4 == 0) {
      writer.skipField();
    } else {
      writer.putFloat4(i);                       // 4
    }

    if (i % 5 == 0) {
      writer.skipField();
    } else {
      writer.putFloat8(i);                       // 5
    }

    if (i % 6 == 0) {
      writer.skipField();
    } else {
      writer.putText(UNICODE_FIELD_PREFIX + i);  // 6
    }

    if (i % 7 == 0) {
      writer.skipField();
    } else {
      writer.putTimestamp(DatumFactory.createTimestamp("2014-04-16 08:48:00").asInt8() + i); // 7
    }

    if (i % 8 == 0) {
      writer.skipField();
    } else {
      writer.putDate(DatumFactory.createDate("2014-04-16").asInt4() + i); // 8
    }

    if (i % 9 == 0) {
      writer.skipField();
    } else {
      writer.putTime(DatumFactory.createTime("08:48:00").asInt8() + i); // 9
    }

    if (i % 10 == 0) {
      writer.skipField();
    } else {
      writer.putInterval(DatumFactory.createInterval((i + 1) + " hours")); // 10
    }

    if (i % 11 == 0) {
      writer.skipField();
    } else {
      writer.putInet4(DatumFactory.createInet4("192.168.0.1").asInt4() + i); // 11
    }

    if (i % 12 == 0) {
      writer.skipField();
    } else {
      writer.putProtoDatum(new ProtobufDatum(ProtoUtil.convertString(i + ""))); // 12
    }

    writer.endRow();
  }

  public static void fillVTuple(int i, VTuple tuple) {
    tuple.put(0, DatumFactory.createBool(i % 1 == 0));
    tuple.put(1, DatumFactory.createInt2((short) 1));
    tuple.put(2, DatumFactory.createInt4(i));
    tuple.put(3, DatumFactory.createInt8(i));
    tuple.put(4, DatumFactory.createFloat4(i));
    tuple.put(5, DatumFactory.createFloat8(i));
    tuple.put(6, DatumFactory.createText(UNICODE_FIELD_PREFIX + i));
    tuple.put(7, DatumFactory.createTimestamp(DatumFactory.createTimestamp("2014-04-16 08:48:00").asInt8() + i)); // 7
    tuple.put(8, DatumFactory.createDate(DatumFactory.createDate("2014-04-16").asInt4() + i)); // 8
    tuple.put(9, DatumFactory.createTime(DatumFactory.createTime("08:48:00").asInt8() + i)); // 9
    tuple.put(10, DatumFactory.createInterval((i + 1) + " hours")); // 10
    tuple.put(11, DatumFactory.createInet4(DatumFactory.createInet4("192.168.0.1").asInt4() + i)); // 11
    tuple.put(12, new ProtobufDatum(ProtoUtil.convertString(i + ""))); // 12;
  }

  public static void validateResults(MemoryRowBlock rowBlock) {
    long readStart = System.currentTimeMillis();
    ZeroCopyTuple tuple = new UnSafeTuple();
    int j = 0;
    OffHeapRowBlockReader reader = new OffHeapRowBlockReader(rowBlock);
    reader.reset();
    while(reader.next(tuple)) {
      validateTupleResult(j, tuple);
      j++;
    }
    assertEquals(rowBlock.rows(), j);
    long readEnd = System.currentTimeMillis();
    LOG.info("Reading takes " + (readEnd - readStart) + " msec");
  }

  public static void validateTupleResult(int j, Tuple t) {
    assertTrue((j % 1 == 0) == t.getBool(0));
    assertTrue(1 == t.getInt2(1));
    assertEquals(j, t.getInt4(2));
    assertEquals(j, t.getInt8(3));
    assertTrue(j == t.getFloat4(4));
    assertTrue(j == t.getFloat8(5));
    assertEquals(UNICODE_FIELD_PREFIX + j, t.getText(6));
    assertEquals(DatumFactory.createTimestamp("2014-04-16 08:48:00").asInt8() + (long) j, t.getInt8(7));
    assertEquals(DatumFactory.createDate("2014-04-16").asInt4() + j, t.getInt4(8));
    assertEquals(DatumFactory.createTime("08:48:00").asInt8() + j, t.getInt8(9));
    assertEquals(DatumFactory.createInterval((j + 1) + " hours"), t.getInterval(10));
    assertEquals(DatumFactory.createInet4("192.168.0.1").asInt4() + j, t.getInt4(11));
    assertEquals(new ProtobufDatum(ProtoUtil.convertString(j + "")), t.getProtobufDatum(12));
  }

  public static void validateNullity(int j, Tuple tuple) {
    if (j == 0) {
      tuple.isBlankOrNull(0);
    } else {
      assertTrue((j % 1 == 0) == tuple.getBool(0));
    }

    if (j % 1 == 0) {
      tuple.isBlankOrNull(1);
    } else {
      assertTrue(1 == tuple.getInt2(1));
    }

    if (j % 2 == 0) {
      tuple.isBlankOrNull(2);
    } else {
      assertEquals(j, tuple.getInt4(2));
    }

    if (j % 3 == 0) {
      tuple.isBlankOrNull(3);
    } else {
      assertEquals(j, tuple.getInt8(3));
    }

    if (j % 4 == 0) {
      tuple.isBlankOrNull(4);
    } else {
      assertTrue(j == tuple.getFloat4(4));
    }

    if (j % 5 == 0) {
      tuple.isBlankOrNull(5);
    } else {
      assertTrue(j == tuple.getFloat8(5));
    }

    if (j % 6 == 0) {
      tuple.isBlankOrNull(6);
    } else {
      assertEquals(UNICODE_FIELD_PREFIX + j, tuple.getText(6));
    }

    if (j % 7 == 0) {
      tuple.isBlankOrNull(7);
    } else {
      assertEquals(DatumFactory.createTimestamp("2014-04-16 08:48:00").asInt8() + (long) j, tuple.getInt8(7));
    }

    if (j % 8 == 0) {
      tuple.isBlankOrNull(8);
    } else {
      assertEquals(DatumFactory.createDate("2014-04-16").asInt4() + j, tuple.getInt4(8));
    }

    if (j % 9 == 0) {
      tuple.isBlankOrNull(9);
    } else {
      assertEquals(DatumFactory.createTime("08:48:00").asInt8() + j, tuple.getInt8(9));
    }

    if (j % 10 == 0) {
      tuple.isBlankOrNull(10);
    } else {
      assertEquals(DatumFactory.createInterval((j + 1) + " hours"), tuple.getInterval(10));
    }

    if (j % 11 == 0) {
      tuple.isBlankOrNull(11);
    } else {
      assertEquals(DatumFactory.createInet4("192.168.0.1").asInt4() + j, tuple.getInt4(11));
    }

    if (j % 12 == 0) {
      tuple.isBlankOrNull(12);
    } else {
      assertEquals(new ProtobufDatum(ProtoUtil.convertString(j + "")), tuple.getProtobufDatum(12));
    }
  }
}