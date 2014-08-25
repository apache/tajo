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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.FileUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.tajo.common.TajoDataTypes.Type;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDirectRowBlock {
  private static final Log LOG = LogFactory.getLog(TestDirectRowBlock.class);

  static String TEXT_FIELD_PREFIX = "가나다_abc_";

  static Schema schema;

  @BeforeClass
  public static void setUp() {
    schema = new Schema();
    schema.addColumn("col0", Type.BOOLEAN);
    schema.addColumn("col1", Type.INT2);
    schema.addColumn("col2", Type.INT4);
    schema.addColumn("col3", Type.INT8);
    schema.addColumn("col4", Type.FLOAT4);
    schema.addColumn("col5", Type.FLOAT8);
    schema.addColumn("col6", Type.TEXT);
    schema.addColumn("col7", Type.TIMESTAMP);
    schema.addColumn("col8", Type.DATE);
    schema.addColumn("col9", Type.TIME);
    schema.addColumn("col10", Type.INET4);
    schema.addColumn("col11", Type.PROTOBUF);
  }

  @Test
  public void testPutAndReadValidation() {
    int vecSize = 1000;

    long allocateStart = System.currentTimeMillis();
    RowOrientedRowBlock rowBlock = new RowOrientedRowBlock(schema, 1024);
    long allocatedEnd = System.currentTimeMillis();
    LOG.info(FileUtil.humanReadableByteCount(rowBlock.totalMem(), true) + " bytes allocated "
        + (allocatedEnd - allocateStart) + " msec");

    UnSafeTuple tuple = new UnSafeTuple();
    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      rowBlock.startRow();
      rowBlock.putBool(i % 1 == 0 ? true : false); // 0
      rowBlock.putInt2((short) 1);                 // 1
      rowBlock.putInt4(i);                         // 2
      rowBlock.putInt8(i);                         // 3
      rowBlock.putFloat4(i);                       // 4
      rowBlock.putFloat8(i);                       // 5
      rowBlock.putText((TEXT_FIELD_PREFIX + i).getBytes());  // 6
      rowBlock.putTimestamp(DatumFactory.createTimestamp("2014-04-16 08:48:00").asInt8() + i); // 7
      rowBlock.putDate(DatumFactory.createDate("2014-04-16").asInt4() + i); // 8
      rowBlock.putTime(DatumFactory.createTime("08:48:00").asInt8() + i); // 9
      rowBlock.putInterval(DatumFactory.createInterval((i + 1) + " hours")); // 10
      rowBlock.putInet4(DatumFactory.createInet4("192.168.0.1").asInt4() + i); // 11
      rowBlock.endRow();

      rowBlock.resetRowCursor();
      int j = 0;
      while(rowBlock.next(tuple)) {
        assertTrue((j % 1 == 0) == tuple.getBool(0));
        assertTrue(1 == tuple.getInt2(1));
        assertEquals(j, tuple.getInt4(2));
        assertEquals(j, tuple.getInt8(3));
        assertTrue(j == tuple.getFloat4(4));
        assertTrue(j == tuple.getFloat8(5));
        assertEquals(new String(TEXT_FIELD_PREFIX + j), tuple.getText(6));
        assertEquals(DatumFactory.createTimestamp("2014-04-16 08:48:00").asInt8() + (long) j, tuple.getInt8(7));
        assertEquals(DatumFactory.createDate("2014-04-16").asInt4() + j, tuple.getInt4(8));
        assertEquals(DatumFactory.createTime("08:48:00").asInt8() + j, tuple.getInt8(9));
        assertEquals(DatumFactory.createInterval((j + 1) + " hours"), tuple.getInterval(10));
        assertEquals(DatumFactory.createInet4("192.168.0.1").asInt4() + j, tuple.getInt4(11));

        j++;
      }
    }
    long writeEnd = System.currentTimeMillis();
    LOG.info("writing and validating take " + (writeEnd - writeStart) + " msec");

    long readStart = System.currentTimeMillis();
    tuple = new UnSafeTuple();
    int j = 0;
    rowBlock.resetRowCursor();
    while(rowBlock.next(tuple)) {
      assertTrue((j % 1 == 0) == tuple.getBool(0));
      assertTrue(1 == tuple.getInt2(1));
      assertEquals(j, tuple.getInt4(2));
      assertEquals(j, tuple.getInt8(3));
      assertTrue(j == tuple.getFloat4(4));
      assertTrue(j == tuple.getFloat8(5));
      assertEquals((TEXT_FIELD_PREFIX + j), tuple.getText(6));
      j++;
    }
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");

    rowBlock.free();
  }

  @Test
  public void testNullityValidation() {
    int vecSize = 1000;

    long allocateStart = System.currentTimeMillis();
    RowOrientedRowBlock rowBlock = new RowOrientedRowBlock(schema, 1024);
    long allocatedEnd = System.currentTimeMillis();
    LOG.info(FileUtil.humanReadableByteCount(rowBlock.totalMem(), true) + " bytes allocated "
        + (allocatedEnd - allocateStart) + " msec");

    UnSafeTuple tuple = new UnSafeTuple();
    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      rowBlock.startRow();

      if (i == 0) {
        rowBlock.skipField();
      } else {
        rowBlock.putBool(i % 1 == 0 ? true : false); // 0
      }
      if (i % 1 == 0) {
        rowBlock.skipField();
      } else {
        rowBlock.putInt2((short) 1);                 // 1
      }

      if (i % 2 == 0) {
        rowBlock.skipField();
      } else {
        rowBlock.putInt4(i);                         // 2
      }

      if (i % 3 == 0) {
        rowBlock.skipField();
      } else {
        rowBlock.putInt8(i);                         // 3
      }

      if (i % 4 == 0) {
        rowBlock.skipField();
      } else {
        rowBlock.putFloat4(i);                       // 4
      }

      if (i % 5 == 0) {
        rowBlock.skipField();
      } else {
        rowBlock.putFloat8(i);                       // 5
      }

      if (i % 6 == 0) {
        rowBlock.skipField();
      } else {
        rowBlock.putText((TEXT_FIELD_PREFIX + i).getBytes());  // 6
      }

      if (i % 7 == 0) {
        rowBlock.skipField();
      } else {
        rowBlock.putTimestamp(DatumFactory.createTimestamp("2014-04-16 08:48:00").asInt8() + i); // 7
      }

      if (i % 8 == 0) {
        rowBlock.skipField();
      } else {
        rowBlock.putDate(DatumFactory.createDate("2014-04-16").asInt4() + i); // 8
      }

      if (i % 9 == 0) {
        rowBlock.skipField();
      } else {
        rowBlock.putTime(DatumFactory.createTime("08:48:00").asInt8() + i); // 9
      }

      if (i % 10 == 0) {
        rowBlock.skipField();
      } else {
        rowBlock.putInterval(DatumFactory.createInterval((i + 1) + " hours")); // 10
      }

      if (i % 11 == 0) {
        rowBlock.skipField();
      } else {
        rowBlock.putInet4(DatumFactory.createInet4("192.168.0.1").asInt4() + i); // 11
      }
      rowBlock.endRow();

      rowBlock.resetRowCursor();
      int j = 0;
      while(rowBlock.next(tuple)) {
        if (j == 0) {
          tuple.isNull(0);
        } else {
          assertTrue((j % 1 == 0) == tuple.getBool(0));
        }

        if (j % 1 == 0) {
          tuple.isNull(1);
        } else {
          assertTrue(1 == tuple.getInt2(1));
        }

        if (j % 2 == 0) {
          tuple.isNull(2);
        } else {
          assertEquals(j, tuple.getInt4(2));
        }

        if (j % 3 == 0) {
          tuple.isNull(3);
        } else {
          assertEquals(j, tuple.getInt8(3));
        }

        if (j % 4 == 0) {
          tuple.isNull(4);
        } else {
          assertTrue(j == tuple.getFloat4(4));
        }

        if (j % 5 == 0) {
          tuple.isNull(5);
        } else {
          assertTrue(j == tuple.getFloat8(5));
        }

        if (j % 6 == 0) {
          tuple.isNull(6);
        } else {
          assertEquals(new String(TEXT_FIELD_PREFIX + j), tuple.getText(6));
        }

        if (j % 7 == 0) {
          tuple.isNull(7);
        } else {
          assertEquals(DatumFactory.createTimestamp("2014-04-16 08:48:00").asInt8() + (long) j, tuple.getInt8(7));
        }

        if (j % 8 == 0) {
          tuple.isNull(8);
        } else {
          assertEquals(DatumFactory.createDate("2014-04-16").asInt4() + j, tuple.getInt4(8));
        }

        if (j % 9 == 0) {
          tuple.isNull(9);
        } else {
          assertEquals(DatumFactory.createTime("08:48:00").asInt8() + j, tuple.getInt8(9));
        }

        if (j % 10 == 0) {
          tuple.isNull(10);
        } else {
          assertEquals(DatumFactory.createInterval((j + 1) + " hours"), tuple.getInterval(10));
        }

        if (j % 11 == 0) {
          tuple.isNull(11);
        } else {
          assertEquals(DatumFactory.createInet4("192.168.0.1").asInt4() + j, tuple.getInt4(11));
        }

        j++;
      }
    }
    long writeEnd = System.currentTimeMillis();
    LOG.info("writing and nullity validating take " + (writeEnd - writeStart) +" msec");

    long readStart = System.currentTimeMillis();
    tuple = new UnSafeTuple();
    int j = 0;
    rowBlock.resetRowCursor();
    while(rowBlock.next(tuple)) {
      if (j == 0) {
        tuple.isNull(0);
      } else {
        assertTrue((j % 1 == 0) == tuple.getBool(0));
      }

      if (j % 1 == 0) {
        tuple.isNull(1);
      } else {
        assertTrue(1 == tuple.getInt2(1));
      }

      if (j % 2 == 0) {
        tuple.isNull(2);
      } else {
        assertEquals(j, tuple.getInt4(2));
      }

      if (j % 3 == 0) {
        tuple.isNull(3);
      } else {
        assertEquals(j, tuple.getInt8(3));
      }

      if (j % 4 == 0) {
        tuple.isNull(4);
      } else {
        assertTrue(j == tuple.getFloat4(4));
      }

      if (j % 5 == 0) {
        tuple.isNull(5);
      } else {
        assertTrue(j == tuple.getFloat8(5));
      }

      if (j % 6 == 0) {
        tuple.isNull(6);
      } else {
        assertEquals(new String(TEXT_FIELD_PREFIX + j), tuple.getText(6));
      }

      if (j % 7 == 0) {
        tuple.isNull(7);
      } else {
        assertEquals(DatumFactory.createTimestamp("2014-04-16 08:48:00").asInt8() + (long) j, tuple.getInt8(7));
      }

      if (j % 8 == 0) {
        tuple.isNull(8);
      } else {
        assertEquals(DatumFactory.createDate("2014-04-16").asInt4() + j, tuple.getInt4(8));
      }

      if (j % 9 == 0) {
        tuple.isNull(9);
      } else {
        assertEquals(DatumFactory.createTime("08:48:00").asInt8() + j, tuple.getInt8(9));
      }

      if (j % 10 == 0) {
        tuple.isNull(10);
      } else {
        assertEquals(DatumFactory.createInterval((j + 1) + " hours"), tuple.getInterval(10));
      }

      if (j % 11 == 0) {
        tuple.isNull(11);
      } else {
        assertEquals(DatumFactory.createInet4("192.168.0.1").asInt4() + j, tuple.getInt4(11));
      }

      j++;
    }
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");

    rowBlock.free();
  }

  @Test
  public void testEmptyRow() {
    int vecSize = 100000;

    long allocateStart = System.currentTimeMillis();
    RowOrientedRowBlock rowBlock = new RowOrientedRowBlock(schema, StorageUnit.MB * 10);
    long allocatedEnd = System.currentTimeMillis();
    LOG.info(FileUtil.humanReadableByteCount(rowBlock.totalMem(), true) + " bytes allocation took "
        + (allocatedEnd - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < vecSize; i++) {
      rowBlock.startRow();
      // empty columns
      rowBlock.endRow();
    }
    long writeEnd = System.currentTimeMillis();
    System.out.println("writing took " + (writeEnd - writeStart) + " msec");

    long readStart = System.currentTimeMillis();
    UnSafeTuple tuple = new UnSafeTuple();
    int j = 0;
    rowBlock.resetRowCursor();
    while(rowBlock.next(tuple)) {
      j++;
    }
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");
    rowBlock.free();

    assertEquals(vecSize, j);
    assertEquals(vecSize, rowBlock.totalRowNum());
  }

  @Test
  public void testSortBenchmark() {
    int vecSize = 100000;

    long allocateStart = System.currentTimeMillis();
    RowOrientedRowBlock rowBlock = new RowOrientedRowBlock(schema, StorageUnit.MB * 10);
    long allocatedEnd = System.currentTimeMillis();
    LOG.info(FileUtil.humanReadableByteCount(rowBlock.totalMem(), true) + " bytes allocated "
        + (allocatedEnd - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    List<UnSafeTuple> unSafeTuples = Lists.newArrayList();
    for (int i = 0; i < vecSize; i++) {
      rowBlock.startRow();
      rowBlock.putBool(i % 1 == 0 ? true : false); // 0
      rowBlock.putInt2((short) 1);                 // 1
      rowBlock.putInt4(i);                         // 2
      rowBlock.putInt8(i);                         // 3
      rowBlock.putFloat4(i);                       // 4
      rowBlock.putFloat8(i);                       // 5
      rowBlock.putText((TEXT_FIELD_PREFIX + i).getBytes());  // 6
      rowBlock.putTimestamp(DatumFactory.createTimestamp("2014-04-16 08:48:00").asInt8() + i); // 7
      rowBlock.putDate(DatumFactory.createDate("2014-04-16").asInt4() + i); // 8
      rowBlock.putTime(DatumFactory.createTime("08:48:00").asInt8() + i); // 9
      rowBlock.putInterval(DatumFactory.createInterval((i + 1) + " hours")); // 10
      rowBlock.putInet4(DatumFactory.createInet4("192.168.0.1").asInt4() + i); // 11
      rowBlock.endRow();
    }
    long writeEnd = System.currentTimeMillis();
    LOG.info("writing took " + (writeEnd - writeStart) + " msec");

    long readStart = System.currentTimeMillis();
    UnSafeTuple tuple = new UnSafeTuple();
    int j = 0;
    rowBlock.resetRowCursor();
    while(rowBlock.next(tuple)) {
      unSafeTuples.add(tuple);
      tuple = new UnSafeTuple();
    }
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");


    SortSpec sortSpec = new SortSpec(new Column("col2", Type.INT4));
    TupleComparator comparator = new TupleComparator(schema, new SortSpec[] {sortSpec});

    long sortStart = System.currentTimeMillis();
    Collections.sort(unSafeTuples, comparator);
    long sortEnd = System.currentTimeMillis();
    LOG.info("sorting took " + (sortEnd - sortStart) + " msec");

    rowBlock.free();
  }

  @Test
  public void testPutVTuple() {
    int vecSize = 1000000;

    List<VTuple> rowBlock = Lists.newArrayList();
    long writeStart = System.currentTimeMillis();
    VTuple tuple;
    for (int i = 0; i < vecSize; i++) {
      tuple = new VTuple(schema.size());
      tuple.put(0, DatumFactory.createBool(i % 1 == 0));
      tuple.put(1, DatumFactory.createInt2((short) 1));
      tuple.put(2, DatumFactory.createInt4(i));
      tuple.put(3, DatumFactory.createInt8(i));
      tuple.put(4, DatumFactory.createFloat4(i));
      tuple.put(5, DatumFactory.createFloat8(i));
      tuple.put(6, DatumFactory.createText((TEXT_FIELD_PREFIX + i).getBytes()));
      rowBlock.add(tuple);
    }
    long writeEnd = System.currentTimeMillis();
    System.out.println(writeEnd - writeStart + " write msec");

    long readStart = System.currentTimeMillis();
    int k = 0;
    for (VTuple t : rowBlock) {
      assertTrue((k % 1 == 0) == t.getBool(0));
      assertTrue(1 == t.getInt2(1));
      assertEquals(k, t.getInt4(2));
      assertEquals(k, t.getInt8(3));
      assertTrue(k == t.getFloat4(4));
      assertTrue(k == t.getFloat8(5));
      assertEquals(new String((TEXT_FIELD_PREFIX + k)), t.getText(6));

      k++;
    }
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");

    int count = 0;
    for (int i = 0; i < rowBlock.size(); i++) {
      for(int j = 0; j < schema.size(); j++ ) {
        if (rowBlock.get(i).contains(j) && rowBlock.get(i).get(j).type() == Type.INT4) {
          count ++;
        }
      }
    }
    // For preventing unnecessary code elimination optimization.
    LOG.info("The number of INT4 values is " + count + ".");
  }
}