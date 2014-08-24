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

package org.apache.tajo.storage.directmem;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.parquet.ParquetAppender;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.apache.tajo.common.TajoDataTypes.Type;
import static org.junit.Assert.*;

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
    System.out.println(FileUtil.humanReadableByteCount(rowBlock.totalMem(), true) + " bytes allocated "
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
    System.out.println(FileUtil.humanReadableByteCount(vecRowBlock.totalMem(), true) + " bytes allocated "
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
      vecRowBlock.setNull(idx % 5, idx, idx % 2);

      assertTrue(vecRowBlock.getNullBit(idx % 5, idx) == (idx % 2 == 0 ? 0 : 1));
    }

    for (int idx : nullIndices) {
      assertTrue(vecRowBlock.getNullBit(idx % 5, idx) == (idx % 2 == 0 ? 0 : 1));
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
    System.out.println(FileUtil.humanReadableByteCount(vecRowBlock.totalMem(), true) + " bytes allocated "
        + (allocateend - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      assertEquals(0, vecRowBlock.getNullBit(0, i));
      vecRowBlock.putInt2(0, i, (short) 1);
      assertEquals(1, vecRowBlock.getNullBit(0, i));

      assertEquals(0, vecRowBlock.getNullBit(1, i));
      vecRowBlock.putInt4(1, i, i);
      assertEquals(1, vecRowBlock.getNullBit(1, i));

      assertEquals(0, vecRowBlock.getNullBit(2, i));
      vecRowBlock.putInt8(2, i, i);
      assertEquals(1, vecRowBlock.getNullBit(2, i));

      assertEquals(0, vecRowBlock.getNullBit(3, i));
      vecRowBlock.putFloat4(3, i, i);
      assertEquals(1, vecRowBlock.getNullBit(3, i));

      assertEquals(0, vecRowBlock.getNullBit(4, i));
      vecRowBlock.putFloat8(4, i, i);
      assertEquals(1, vecRowBlock.getNullBit(4, i));
    }
    long writeEnd = System.currentTimeMillis();
    System.out.println(writeEnd - writeStart + " write msec");

    Set<Integer> nullIdx = Sets.newHashSet();
    Random rnd = new Random(System.currentTimeMillis());

    for (int i = 0; i < 200; i++) {
      int idx = rnd.nextInt(vecSize);
      nullIdx.add(idx);
      vecRowBlock.unsetNullBit(0, idx);
      assertTrue(vecRowBlock.getNullBit(0, idx) == 0);
    }

    for (int i = 0; i < 200; i++) {
      int idx = rnd.nextInt(vecSize);
      nullIdx.add(idx);
      vecRowBlock.unsetNullBit(1, idx);
      assertTrue(vecRowBlock.getNullBit(1, idx) == 0);
    }

    for (int i = 0; i < vecSize; i++) {
      if (nullIdx.contains(i)) {
        assertTrue(vecRowBlock.getNullBit(0, i) == 0 || vecRowBlock.getNullBit(1, i) == 0);
      } else {
        if (!(vecRowBlock.getNullBit(0, i) == 1 && vecRowBlock.getNullBit(1, i) == 1)) {
          System.out.println("idx: " + i);
          System.out.println("nullIdx: " + nullIdx.contains(new Integer(i)));
          System.out.println("1st null vec: " + vecRowBlock.getNullBit(0, i));
          System.out.println("2st null vec: " + vecRowBlock.getNullBit(1, i));
          fail();
        }
      }
    }


    long nullVector = VecRowBlock.allocateNullVector(vecSize);
    VectorUtil.mapAndBitmapVector(vecSize, nullVector, vecRowBlock.getNullVecPtr(0), vecRowBlock.getNullVecPtr(1));

    for (int i = 0; i < vecSize; i++) {
      if (nullIdx.contains(i)) {
        assertTrue(VectorUtil.isNull(nullVector, i) == 0);
      } else {
        if (VectorUtil.isNull(nullVector, i) == 0) {
          System.out.println("idx: " + i);
          System.out.println("nullIdx: " + nullIdx.contains(new Integer(i)));
          System.out.println("1st null vec: " + vecRowBlock.getNullBit(0, i));
          System.out.println("2st null vec: " + vecRowBlock.getNullBit(1, i));
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
    System.out.println(FileUtil.humanReadableByteCount(vecRowBlock.totalMem(), true) + " bytes allocated "
        + (allocateend - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      byte [] bytes = ("abcdef_" + i).getBytes(Charset.defaultCharset());
      vecRowBlock.putVarText(5, i, bytes, 0, bytes.length);
    }
    long writeEnd = System.currentTimeMillis();
    System.out.println(writeEnd - writeStart + " write msec");

    byte [] str = new byte[50];
    long readStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      int len = vecRowBlock.getVarText(5, i, str);
      assertEquals(("abcdef_" + i), new String(str, 0, len));
    }
    long readEnd = System.currentTimeMillis();
    System.out.println(readEnd - readStart + " read msec");

    System.out.println("Total Size: " + FileUtil.humanReadableByteCount(vecRowBlock.totalMem(), true));
    System.out.println("Fixed Area Size: " + FileUtil.humanReadableByteCount(vecRowBlock.fixedAreaMemory(), true));
    System.out.println("Variable Area Size: " + FileUtil.humanReadableByteCount(vecRowBlock.variableAreaMemory(), true));
    vecRowBlock.free();
  }

  private static final Schema schema;
  private static final long totalTupleNum = 1024;
  private static final int vectorSize = 1024;

  static {
    schema = new Schema();
    schema.addColumn("col0", Type.BOOLEAN);
    schema.addColumn("col1", Type.INT2);
    schema.addColumn("col2", Type.INT4);
    schema.addColumn("col3", Type.INT8);
    schema.addColumn("col4", Type.FLOAT4);
    schema.addColumn("col5", Type.FLOAT8);
    schema.addColumn("col6", Type.TEXT);
    schema.addColumn("col7", Type.TEXT);
  }

  public FileFragment generateParquetViaTuples(TableMeta meta) throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path("file:///tmp/parquet-" + System.currentTimeMillis());
    ParquetAppender appender = new ParquetAppender(conf, schema, meta, path);
    appender.init();

    Tuple tuple = new VTuple(schema.size());
    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < totalTupleNum; i++) {
      tuple.put(0, DatumFactory.createBool(i % 2 == 1 ? true : false));
      tuple.put(1, DatumFactory.createInt2((short) 1));
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

    return new FileFragment("tb1", path, 0, fileSize);
  }

  @Test
  public void testMod() {
//    x % 2 == x & 1
//    x % 4 == x & 3
//    x % 8 == x & 7.
    long UNSIGNED_MASK = 0x7fffffffffffffffL;
    long x = -120312890123798129L & UNSIGNED_MASK;
    long y = 4;

    System.out.println("java: " + (x) % y);
    System.out.println("bitwise: " + ((x) & y - 1));
  }
}
