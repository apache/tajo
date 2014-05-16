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

package org.apache.tajo.storage.newtuple;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.newtuple.map.MapAddInt8ColInt8ColOp;
import org.apache.tajo.storage.newtuple.map.VecFuncStrcmpStrStrColx2;
import org.apache.tajo.storage.newtuple.map.SelStrEqStrColStrColOp;
import org.apache.tajo.storage.parquet.ParquetAppender;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.VecRowParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static org.apache.tajo.common.TajoDataTypes.Type;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestVecRowBlock {
  @Test
  public void testPutInt() {
    Schema schema = new Schema();
    schema.addColumn("col1", Type.INT2);
    schema.addColumn("col2", Type.INT4);
    schema.addColumn("col3", Type.INT8);
    schema.addColumn("col4", Type.FLOAT4);
    schema.addColumn("col5", Type.FLOAT8);

    int vecSize = 4096;

    long allocateStart = System.currentTimeMillis();
    VecRowBlock rowBlock = new VecRowBlock(schema, vecSize);
    long allocatedEnd = System.currentTimeMillis();
    System.out.println(FileUtil.humanReadableByteCount(rowBlock.size(), true) + " bytes allocated "
        + (allocatedEnd - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      rowBlock.putInt2(0, i, (short) 1);
      rowBlock.putInt4(1, i, i);
      rowBlock.putInt8(2, i, i);
      rowBlock.putFloat4(3, i, i);
      rowBlock.putFloat8(4, i, i);
    }
    long writeEnd = System.currentTimeMillis();
    System.out.println(writeEnd - writeStart + " write msec");

    long readStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      assertTrue(1 == rowBlock.getInt2(0, i));
      assertEquals(i, rowBlock.getInt4(1, i));
      assertEquals(i, rowBlock.getInt8(2, i));
      assertTrue(i == rowBlock.getFloat4(3, i));
      assertTrue(i == rowBlock.getFloat8(4, i));
    }
    long readEnd = System.currentTimeMillis();
    System.out.println(readEnd - readStart + " read msec");

    rowBlock.free();
  }

  @Test
  public void testAddTest() {
    Schema schema = new Schema();
    schema.addColumn("col1", Type.INT2);
    schema.addColumn("col2", Type.INT4);
    schema.addColumn("col3", Type.INT8);
    schema.addColumn("col4", Type.FLOAT4);
    schema.addColumn("col5", Type.FLOAT8);

    int vecSize = 1024;

    long allocateStart = System.currentTimeMillis();
    VecRowBlock vecRowBlock = new VecRowBlock(schema, vecSize);
    long allocateend = System.currentTimeMillis();
    System.out.println(FileUtil.humanReadableByteCount(vecRowBlock.size(), true) + " bytes allocated "
        + (allocateend - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      vecRowBlock.putInt2(0, i, (short) 1);
      vecRowBlock.putInt4(1, i, i);
      vecRowBlock.putInt8(2, i, i);
      vecRowBlock.putFloat4(3, i, i);
      vecRowBlock.putFloat8(4, i, i);
    }
    long writeEnd = System.currentTimeMillis();
    System.out.println(writeEnd - writeStart + " write msec");

    long readStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      assertTrue(1 == vecRowBlock.getInt2(0, i));
      assertEquals(i, vecRowBlock.getInt4(1, i));
      assertEquals(i, vecRowBlock.getInt8(2, i));
      assertTrue(i == vecRowBlock.getFloat4(3, i));
      assertTrue(i == vecRowBlock.getFloat8(4, i));
    }
    long readEnd = System.currentTimeMillis();
    System.out.println(readEnd - readStart + " read msec");

    MapAddInt8ColInt8ColOp op = new MapAddInt8ColInt8ColOp();

    long result = UnsafeUtil.allocVector(Type.INT8, vecSize);
    op.map(vecSize, result, vecRowBlock.getValueVecPtr(2), vecRowBlock.getValueVecPtr(2), 0, 0);

    for (int i = 0; i < vecSize; i++) {
      assertEquals(UnsafeUtil.getLong(result, i), vecRowBlock.getInt8(2, i) * 2);
    }


    long selVec = UnsafeUtil.allocVector(Type.INT8, 3);
    int idx = 0;
    UnsafeUtil.putInt(selVec, idx++, 3);
    UnsafeUtil.putInt(selVec, idx++, 7);
    UnsafeUtil.putInt(selVec, idx++, 9);

    op.map(3, result, vecRowBlock.getValueVecPtr(2), vecRowBlock.getValueVecPtr(2), 0, selVec);

    for (int i = 0; i < 3; i++) {
      System.out.println(UnsafeUtil.getLong(result, i));
    }

    vecRowBlock.free();
    UnsafeUtil.free(result);
    UnsafeUtil.free(selVec);
  }

  @Test
  public void testStrCmpTest() {
    Schema schema = new Schema();
    schema.addColumn("col0", Type.BOOLEAN);
    schema.addColumn("col1", Type.INT2);
    schema.addColumn("col2", Type.INT4);
    schema.addColumn("col3", Type.INT8);
    schema.addColumn("col4", Type.FLOAT4);
    schema.addColumn("col5", Type.FLOAT8);
    schema.addColumn("col6", Type.TEXT);
    schema.addColumn("col7", Type.TEXT);

    int vecSize = 1024;

    long allocateStart = System.currentTimeMillis();
    VecRowBlock vecRowBlock = new VecRowBlock(schema, vecSize);
    long allocateend = System.currentTimeMillis();
    System.out.println(FileUtil.humanReadableByteCount(vecRowBlock.size(), true) + " bytes allocated "
        + (allocateend - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      vecRowBlock.putBool(0, i, i % 2);
      vecRowBlock.putInt2(1, i, (short) 1);
      vecRowBlock.putInt4(2, i, i);
      vecRowBlock.putInt8(3, i, i);
      vecRowBlock.putFloat4(4, i, i);
      vecRowBlock.putFloat8(5, i, i);
      vecRowBlock.putText(6, i, "colabcdefghijklmnopqrstu1".getBytes());
      vecRowBlock.putText(7, i, "colabcdefghijklmnopqrstu2".getBytes());
    }
    long writeEnd = System.currentTimeMillis();
    System.out.println(writeEnd - writeStart + " write msec");

    long readStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      assertEquals(i % 2, vecRowBlock.getBool(0, i));
      assertTrue(1 == vecRowBlock.getInt2(1, i));
      assertEquals(i, vecRowBlock.getInt4(2, i));
      assertEquals(i, vecRowBlock.getInt8(3, i));
      assertTrue(i == vecRowBlock.getFloat4(4, i));
      assertTrue(i == vecRowBlock.getFloat8(5, i));
      assertEquals("colabcdefghijklmnopqrstu1", (vecRowBlock.getString(6, i)));
      assertEquals("colabcdefghijklmnopqrstu2", (vecRowBlock.getString(7, i)));
    }
    long readEnd = System.currentTimeMillis();
    System.out.println(readEnd - readStart + " read msec");

    VecFuncStrcmpStrStrColx2 op = new VecFuncStrcmpStrStrColx2();

    long resPtr = UnsafeUtil.allocVector(Type.INT4, vecSize);
    op.map(vecSize, resPtr, vecRowBlock.getValueVecPtr(6), vecRowBlock.getValueVecPtr(7), 0, 0);

    for (int i = 0; i < vecSize; i++) {
      assertTrue(UnsafeUtil.getInt(resPtr, i) < 0);
    }
    vecRowBlock.free();
    UnsafeUtil.free(resPtr);
  }

  @Test
  public void testStrCmpEqTest() {
    Schema schema = new Schema();
    schema.addColumn("col1", Type.INT2);
    schema.addColumn("col2", Type.INT4);
    schema.addColumn("col3", Type.INT8);
    schema.addColumn("col4", Type.FLOAT4);
    schema.addColumn("col5", Type.FLOAT8);
    schema.addColumn("col6", Type.TEXT);
    schema.addColumn("col7", Type.TEXT);

    int vecSize = 1024;

    long allocateStart = System.currentTimeMillis();
    VecRowBlock vecRowBlock = new VecRowBlock(schema, vecSize);
    long allocateend = System.currentTimeMillis();
    System.out.println(FileUtil.humanReadableByteCount(vecRowBlock.size(), true) + " bytes allocated "
        + (allocateend - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      vecRowBlock.putInt2(0, i, (short) 1);
      vecRowBlock.putInt4(1, i, i);
      vecRowBlock.putInt8(2, i, i);
      vecRowBlock.putFloat4(3, i, i);
      vecRowBlock.putFloat8(4, i, i);
      if (i % 8 == 0) {
        vecRowBlock.putText(5, i, "1998-09-01".getBytes());
      } else {
        vecRowBlock.putText(5, i, "1111-11-01".getBytes());
      }
      vecRowBlock.putText(6, i, "1998-09-01".getBytes());
    }
    long writeEnd = System.currentTimeMillis();
    System.out.println(writeEnd - writeStart + " write msec");

    long readStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      assertTrue(1 == vecRowBlock.getInt2(0, i));
      assertEquals(i, vecRowBlock.getInt4(1, i));
      assertEquals(i, vecRowBlock.getInt8(2, i));
      assertTrue(i == vecRowBlock.getFloat4(3, i));
      assertTrue(i == vecRowBlock.getFloat8(4, i));
      if (i % 8 == 0) {
        assertEquals("1998-09-01", (vecRowBlock.getString(5, i)));
      } else {
        assertEquals("1111-11-01", (vecRowBlock.getString(5, i)));
      }
      assertEquals("1998-09-01", (vecRowBlock.getString(6, i)));
    }
    long readEnd = System.currentTimeMillis();
    System.out.println(readEnd - readStart + " read msec");

    SelStrEqStrColStrColOp op = new SelStrEqStrColStrColOp();

    long resPtr = UnsafeUtil.allocVector(Type.INT4, vecSize);
    int selNum = op.sel(vecSize, resPtr, vecRowBlock.getValueVecPtr(5), vecRowBlock.getValueVecPtr(6), 0, 0);

    for (int i = 0; i < selNum; i++) {
      System.out.println(UnsafeUtil.getInt(resPtr, i));
    }
    vecRowBlock.free();
    UnsafeUtil.free(resPtr);
  }

  @Test
  public void testSetNullIsNull() {
    Schema schema = new Schema();
    schema.addColumn("col1", Type.INT2);
    schema.addColumn("col2", Type.INT4);
    schema.addColumn("col3", Type.INT8);
    schema.addColumn("col4", Type.FLOAT4);
    schema.addColumn("col5", Type.FLOAT8);

    int vecSize = 1024;

    long allocateStart = System.currentTimeMillis();
    VecRowBlock vecRowBlock = new VecRowBlock(schema, vecSize);
    long allocateend = System.currentTimeMillis();
    System.out.println(FileUtil.humanReadableByteCount(vecRowBlock.size(), true) + " bytes allocated "
        + (allocateend - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      vecRowBlock.putInt2(0, i, (short) 1);
      vecRowBlock.putInt4(1, i, i);
      vecRowBlock.putInt8(2, i, i);
      vecRowBlock.putFloat4(3, i, i);
      vecRowBlock.putFloat8(4, i, i);
    }
    long writeEnd = System.currentTimeMillis();
    System.out.println(writeEnd - writeStart + " write msec");

    List<Integer> nullIndices = Lists.newArrayList();
    Random rnd = new Random(System.currentTimeMillis());

    for (int i = 0; i < 500; i++) {
      int idx = rnd.nextInt(vecSize);
      nullIndices.add(idx);
      vecRowBlock.setNull(idx % 5, idx);

      assertTrue(vecRowBlock.isNull(idx % 5, idx) == 0);
    }

    for (int idx : nullIndices) {
      assertTrue(vecRowBlock.isNull(idx % 5, idx) == 0);
    }
    vecRowBlock.free();
  }

  @Test
  public void testNullify() {
    Schema schema = new Schema();
    schema.addColumn("col1", Type.INT2);
    schema.addColumn("col2", Type.INT4);
    schema.addColumn("col3", Type.INT8);
    schema.addColumn("col4", Type.FLOAT4);
    schema.addColumn("col5", Type.FLOAT8);

    int vecSize = 1024;

    long allocateStart = System.currentTimeMillis();
    VecRowBlock vecRowBlock = new VecRowBlock(schema, vecSize);

    long allocateend = System.currentTimeMillis();
    System.out.println(FileUtil.humanReadableByteCount(vecRowBlock.size(), true) + " bytes allocated "
        + (allocateend - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      vecRowBlock.putInt2(0, i, (short) 1);
      vecRowBlock.putInt4(1, i, i);
      vecRowBlock.putInt8(2, i, i);
      vecRowBlock.putFloat4(3, i, i);
      vecRowBlock.putFloat8(4, i, i);
    }
    long writeEnd = System.currentTimeMillis();
    System.out.println(writeEnd - writeStart + " write msec");

    List<Integer> nullIdx = Lists.newArrayList();
    Random rnd = new Random(System.currentTimeMillis());

    for (int i = 0; i < 100; i++) {
      int idx = rnd.nextInt(vecSize);
      nullIdx.add(idx);
      vecRowBlock.setNull(0, idx);
      assertTrue(vecRowBlock.isNull(0, idx) == 0);
    }

    for (int i = 0; i < 100; i++) {
      int idx = rnd.nextInt(vecSize);
      nullIdx.add(idx);
      vecRowBlock.setNull(1, idx);
      assertTrue(vecRowBlock.isNull(1, idx) == 0);
    }

    for (int i = 0; i < vecSize; i++) {
      if (nullIdx.contains(i)) {
        assertTrue(vecRowBlock.isNull(0, i) == 0 || vecRowBlock.isNull(1, i) == 0);
      } else {
        if (!(vecRowBlock.isNull(0, i) == 1 && vecRowBlock.isNull(1, i) == 1)) {
          System.out.println("idx: " + i);
          System.out.println("nullIdx: " + nullIdx.contains(new Integer(i)));
          System.out.println("1st null vec: " + vecRowBlock.isNull(0, i));
          System.out.println("2st null vec: " + vecRowBlock.isNull(1, i));
          fail();
        }
      }
    }


    long nullVector = VecRowBlock.allocateNullVector(vecSize);
    VectorUtil.nullify(vecSize, nullVector, vecRowBlock.getNullVecPtr(0), vecRowBlock.getNullVecPtr(1));

    for (int i = 0; i < vecSize; i++) {
      if (nullIdx.contains(i)) {
        assertTrue(VectorUtil.isNull(nullVector, i) == 0);
      } else {
        if (VectorUtil.isNull(nullVector, i) == 0) {
          System.out.println("idx: " + i);
          System.out.println("nullIdx: " + nullIdx.contains(new Integer(i)));
          System.out.println("1st null vec: " + vecRowBlock.isNull(0, i));
          System.out.println("2st null vec: " + vecRowBlock.isNull(1, i));
          fail();
        }
      }
    }

    vecRowBlock.free();
  }

  //@Test
  public void testPutIntInTuple() {
    Schema schema = new Schema();
    schema.addColumn("col1", Type.INT4);
    schema.addColumn("col2", Type.INT4);

    int vecSize = 100000000;

    long allocateStart = System.currentTimeMillis();
    Tuple[] tuples = new Tuple[vecSize];
    for (int i = 0; i < tuples.length; i++) {
      tuples[i] = new VTuple(2);
    }
    long allocateend = System.currentTimeMillis();
    System.out.println((allocateend - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      tuples[i].put(0, DatumFactory.createInt4(i));
      tuples[i].put(1, DatumFactory.createInt4(i));
    }
    long writeEnd = System.currentTimeMillis();
    System.out.println(writeEnd - writeStart + " write msec");

    long readStart = System.currentTimeMillis();
    long sum = 0;
    for (int i = 0; i < vecSize; i++) {
      assertEquals(i, tuples[i].getInt4(0));
      assertEquals(i, tuples[i].getInt4(1));
    }
    System.out.println(sum);
    long readEnd = System.currentTimeMillis();
    System.out.println(readEnd - readStart + " read msec");
  }

  @Test
  public void testBzero() {
    int bytes = 100000000;
    long ptr = UnsafeUtil.alloc(bytes);
    UnsafeUtil.bzero(ptr, bytes);
    for (int i = 0; i < bytes; i++) {
      assertEquals(0, UnsafeUtil.unsafe.getByte(ptr + i));
    }
    UnsafeUtil.free(ptr);
  }

  @Test
  public void testVariableLength() {
    Schema schema = new Schema();
    schema.addColumn("col1", Type.INT2);
    schema.addColumn("col2", Type.INT4);
    schema.addColumn("col3", Type.INT8);
    schema.addColumn("col4", Type.FLOAT4);
    schema.addColumn("col5", Type.FLOAT8);
    schema.addColumn("col6", Type.TEXT);

    int vecSize = 1024;

    long allocateStart = System.currentTimeMillis();
    VecRowBlock vecRowBlock = new VecRowBlock(schema, vecSize);

    long allocateend = System.currentTimeMillis();
    System.out.println(FileUtil.humanReadableByteCount(vecRowBlock.size(), true) + " bytes allocated "
        + (allocateend - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      byte [] bytes = ("abcdef_" + i).getBytes(Charset.defaultCharset());
      vecRowBlock.putText(5, i, bytes, 0, bytes.length);
    }
    long writeEnd = System.currentTimeMillis();
    System.out.println(writeEnd - writeStart + " write msec");

    byte [] str = new byte[50];
    long readStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      int len = vecRowBlock.getText(5, i, str);
      assertEquals(("abcdef_" + i), new String(str, 0, len));
    }
    long readEnd = System.currentTimeMillis();
    System.out.println(readEnd - readStart + " read msec");

    System.out.println("Total Size: " + FileUtil.humanReadableByteCount(vecRowBlock.size(), true));
    System.out.println("Fixed Area Size: " + FileUtil.humanReadableByteCount(vecRowBlock.getFixedAreaSize(), true));
    System.out.println("Variable Area Size: " + FileUtil.humanReadableByteCount(vecRowBlock.getVariableAreaSize(), true));
    vecRowBlock.free();
  }

  @Test
  public void testTupleParquetReadWrite() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col0", Type.BOOLEAN);
    schema.addColumn("col1", Type.INT2);
    schema.addColumn("col2", Type.INT4);
    schema.addColumn("col3", Type.INT8);
    schema.addColumn("col4", Type.FLOAT4);
    schema.addColumn("col5", Type.FLOAT8);
    schema.addColumn("col6", Type.TEXT);
    schema.addColumn("col7", Type.TEXT);

    int vecSize = 1024 * 10000;

    Configuration conf = new Configuration();
    Path path = new Path("file:///tmp/parquet-" + System.currentTimeMillis());
    KeyValueSet KeyValueSet = StorageUtil.newPhysicalProperties(CatalogProtos.StoreType.PARQUET);
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.PARQUET, KeyValueSet);
    meta.putOption(ParquetOutputFormat.COMPRESSION, CompressionCodecName.SNAPPY.name());
    ParquetAppender appender = new ParquetAppender(conf, schema, meta, path);
    appender.init();

    Tuple tuple = new VTuple(schema.size());
    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      tuple.put(0, DatumFactory.createBool(i % 2));
      tuple.put(1, DatumFactory.createInt2((short) i));
      tuple.put(2, DatumFactory.createInt4(i));
      tuple.put(3, DatumFactory.createInt8(i));
      tuple.put(4, DatumFactory.createFloat4(i));
      tuple.put(5, DatumFactory.createFloat8(i));
      tuple.put(6, DatumFactory.createText("colabcdefghijklmnopqrstu1".getBytes()));
      tuple.put(7, DatumFactory.createText("colabcdefghijklmnopqrstu2".getBytes()));
      appender.addTuple(tuple);
    }
    appender.close();
    long writeEnd = System.currentTimeMillis();

    FileSystem fs = RawLocalFileSystem.get(new Configuration());
    long fileSize = fs.getFileStatus(path).getLen();
    System.out.println((writeEnd - writeStart) +
        " msec, total file size: " + FileUtil.humanReadableByteCount(fileSize, true));
  }

  @Test
  public void testParquetReadWrite() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col0", Type.BOOLEAN);
    schema.addColumn("col1", Type.INT2);
    schema.addColumn("col2", Type.INT4);
    schema.addColumn("col3", Type.INT8);
    schema.addColumn("col4", Type.FLOAT4);
    schema.addColumn("col5", Type.FLOAT8);
    schema.addColumn("col6", Type.TEXT);
    schema.addColumn("col7", Type.TEXT);

    int vecSize = 1024 * 10000;

    long allocateStart = System.currentTimeMillis();
    VecRowBlock vecRowBlock = new VecRowBlock(schema, vecSize);
    long allocateend = System.currentTimeMillis();
    System.out.println(FileUtil.humanReadableByteCount(vecRowBlock.size(), true) + " bytes allocated "
        + (allocateend - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      vecRowBlock.putBool(0, i, i % 2);
      vecRowBlock.putInt2(1, i, (short) 1);
      vecRowBlock.putInt4(2, i, i);
      vecRowBlock.putInt8(3, i, i);
      vecRowBlock.putFloat4(4, i, i);
      vecRowBlock.putFloat8(5, i, i);
      vecRowBlock.putText(6, i, "colabcdefghijklmnopqrstu1".getBytes());
      vecRowBlock.putText(7, i, "colabcdefghijklmnopqrstu2".getBytes());
    }
    long writeEnd = System.currentTimeMillis();
    System.out.println(writeEnd - writeStart + " write msec");


    Path path = new Path("file:///tmp/parquet-" + System.currentTimeMillis());

    VecRowParquetWriter writer = new VecRowParquetWriter(
        path,
        schema,
        new HashMap<String, String>(),
        CompressionCodecName.SNAPPY, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE);

    long startParquetWrite = System.currentTimeMillis();
    writer.write(vecRowBlock);
    writer.close();
    long endParquetWrite = System.currentTimeMillis();

    FileSystem fs = RawLocalFileSystem.get(new Configuration());
    long fileSize = fs.getFileStatus(path).getLen();
    System.out.println((endParquetWrite - startParquetWrite) +
        " msec, total file size: " + FileUtil.humanReadableByteCount(fileSize, true));
  }
}
