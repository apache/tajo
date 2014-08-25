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

package org.apache.tajo.storage.raw;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.directmem.RowOrientedRowBlock;
import org.apache.tajo.storage.directmem.UnSafeTuple;
import org.apache.tajo.storage.rawfile.DirectRawFileScanner;
import org.apache.tajo.storage.rawfile.DirectRawFileWriter;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDirectRawFile {
  private static final Log LOG = LogFactory.getLog(TestDirectRawFile.class);

  static String TEXT_FIELD_PREFIX = "가나다_abc_";

  static Schema schema;

  @BeforeClass
  public static void setUp() {
    schema = new Schema();
    schema.addColumn("col0", TajoDataTypes.Type.BOOLEAN);
    schema.addColumn("col1", TajoDataTypes.Type.INT2);
    schema.addColumn("col2", TajoDataTypes.Type.INT4);
    schema.addColumn("col3", TajoDataTypes.Type.INT8);
    schema.addColumn("col4", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("col5", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("col6", TajoDataTypes.Type.TEXT);
    schema.addColumn("col7", TajoDataTypes.Type.TIMESTAMP);
    schema.addColumn("col8", TajoDataTypes.Type.DATE);
    schema.addColumn("col9", TajoDataTypes.Type.TIME);
    schema.addColumn("col10", TajoDataTypes.Type.INET4);
    schema.addColumn("col11", TajoDataTypes.Type.PROTOBUF);
  }

  @Test
  public void testRWForAllTypes() throws IOException {
    int rowNum = 10;

    long allocateStart = System.currentTimeMillis();
    RowOrientedRowBlock rowBlock = new RowOrientedRowBlock(schema, StorageUnit.MB * 1);
    long allocatedEnd = System.currentTimeMillis();
    LOG.info(FileUtil.humanReadableByteCount(rowBlock.totalMem(), true) + " bytes allocated "
        + (allocatedEnd - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < rowNum; i++) {
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
    LOG.info("writing and validating take " + (writeEnd - writeStart) + " msec");


    Path testDir = CommonTestingUtil.getTestDir();
    Path outputFile = new Path(testDir, "output.draw");
    TajoConf conf = new TajoConf();
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.DIRECTRAW);
    DirectRawFileWriter writer = new DirectRawFileWriter(conf, schema, meta, outputFile);
    writer.init();
    writer.writeRowBlock(rowBlock);
    writer.close();

    rowBlock.free();

    FileSystem fs = FileSystem.getLocal(conf);
    FileStatus status = fs.getFileStatus(outputFile);
    assertTrue(status.getLen() > 0);
    LOG.info("Written file size: " + FileUtil.humanReadableByteCount(status.getLen(), false));

    RowOrientedRowBlock readBlock = new RowOrientedRowBlock(schema, StorageUnit.MB * 1);
    DirectRawFileScanner reader = new DirectRawFileScanner(conf, schema, meta, outputFile);
    reader.init();

    long readStart = System.currentTimeMillis();
    UnSafeTuple tuple = new UnSafeTuple();
    int j = 0;

    while(reader.next(readBlock)) {
      readBlock.resetRowCursor();
      while (readBlock.next(tuple)) {
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
    LOG.info("Total read rows: " + j);
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");
    reader.close();
    readBlock.free();

    assertEquals(rowNum, j);
  }

  @Test
  public void testNullityValidation() throws IOException {
    int rowNum = 1000;

    long allocateStart = System.currentTimeMillis();
    RowOrientedRowBlock rowBlock = new RowOrientedRowBlock(schema, 1024);
    long allocatedEnd = System.currentTimeMillis();
    LOG.info(FileUtil.humanReadableByteCount(rowBlock.totalMem(), true) + " bytes allocated "
        + (allocatedEnd - allocateStart) + " msec");

    UnSafeTuple tuple = new UnSafeTuple();
    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < rowNum; i++) {
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

    Path testDir = CommonTestingUtil.getTestDir();
    Path outputFile = new Path(testDir, "output.draw");
    TajoConf conf = new TajoConf();
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.DIRECTRAW);
    DirectRawFileWriter writer = new DirectRawFileWriter(conf, schema, meta, outputFile);
    writer.init();
    writer.writeRowBlock(rowBlock);
    writer.close();

    rowBlock.free();


    FileSystem fs = FileSystem.getLocal(conf);
    FileStatus status = fs.getFileStatus(outputFile);
    assertTrue(status.getLen() > 0);
    LOG.info("Written file size: " + FileUtil.humanReadableByteCount(status.getLen(), false));

    RowOrientedRowBlock readBlock = new RowOrientedRowBlock(schema, StorageUnit.MB * 1);
    DirectRawFileScanner reader = new DirectRawFileScanner(conf, schema, meta, outputFile);
    reader.init();

    long readStart = System.currentTimeMillis();
    tuple = new UnSafeTuple();
    int j = 0;

    while(reader.next(readBlock)) {
      readBlock.resetRowCursor();
      while (readBlock.next(tuple)) {

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
    LOG.info("Total read rows: " + j);
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");
    reader.close();
    readBlock.free();

    assertEquals(rowNum, j);
  }
}
