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
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.directmem.RowOrientedRowBlock;
import org.apache.tajo.storage.directmem.TestRowOrientedRowBlock;
import org.apache.tajo.storage.directmem.UnSafeTuple;
import org.apache.tajo.storage.rawfile.DirectRawFileScanner;
import org.apache.tajo.storage.rawfile.DirectRawFileWriter;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tajo.storage.directmem.TestRowOrientedRowBlock.*;
import static org.junit.Assert.*;

public class TestDirectRawFile {
  private static final Log LOG = LogFactory.getLog(TestDirectRawFile.class);

  public static Path writeRowBlock(TajoConf conf, TableMeta meta, RowOrientedRowBlock rowBlock, Path outputFile)
      throws IOException {
    DirectRawFileWriter writer = new DirectRawFileWriter(conf, schema, meta, outputFile);
    writer.init();
    writer.writeRowBlock(rowBlock);
    writer.close();

    FileSystem fs = FileSystem.getLocal(conf);
    FileStatus status = fs.getFileStatus(outputFile);
    assertTrue(status.getLen() > 0);
    LOG.info("Written file size: " + FileUtil.humanReadableByteCount(status.getLen(), false));

    return outputFile;
  }

  public static Path writeRowBlock(TajoConf conf, TableMeta meta, RowOrientedRowBlock rowBlock) throws IOException {
    Path testDir = CommonTestingUtil.getTestDir();
    Path outputFile = new Path(testDir, "output.draw");
    return writeRowBlock(conf, meta, rowBlock, outputFile);
  }

  //@Test
  public void testRWForAllTypes() throws IOException {
    int rowNum = 50000;

    RowOrientedRowBlock rowBlock = TestRowOrientedRowBlock.createRowBlock(rowNum);
    TajoConf conf = new TajoConf();
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.DIRECTRAW);
    Path outputFile = writeRowBlock(conf, meta, rowBlock);
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
        TestRowOrientedRowBlock.validateTupleResult(j, tuple);
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
  public void testRWForAllTypesWithNextTuple() throws IOException {
    int rowNum = 10000;

    RowOrientedRowBlock rowBlock = TestRowOrientedRowBlock.createRowBlock(rowNum);

    TajoConf conf = new TajoConf();
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.DIRECTRAW);
    Path outputFile = writeRowBlock(conf, meta, rowBlock);
    rowBlock.free();

    DirectRawFileScanner reader = new DirectRawFileScanner(conf, schema, meta, outputFile);
    reader.init();

    long readStart = System.currentTimeMillis();
    int j = 0;
    Tuple tuple;
    while ((tuple = reader.next()) != null) {
      TestRowOrientedRowBlock.validateTupleResult(j, tuple);
      j++;
    }

    LOG.info("Total read rows: " + j);
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");
    reader.close();
    assertEquals(rowNum, j);
  }

  @Test
  public void testRepeatedScan() throws IOException {
    int rowNum = 2;

    RowOrientedRowBlock rowBlock = TestRowOrientedRowBlock.createRowBlock(rowNum);

    TajoConf conf = new TajoConf();
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.DIRECTRAW);
    Path outputFile = writeRowBlock(conf, meta, rowBlock);

    rowBlock.free();

    DirectRawFileScanner reader = new DirectRawFileScanner(conf, schema, meta, outputFile);
    reader.init();

    int j = 0;
    while (reader.next() != null) {
      j++;
    }
    assertEquals(rowNum, j);

    for (int i = 0; i < 5; i++) {
      assertNull(reader.next());
    }

    reader.close();
  }

  @Test
  public void testReset() throws IOException {
    int rowNum = 2;

    RowOrientedRowBlock rowBlock = TestRowOrientedRowBlock.createRowBlock(rowNum);

    TajoConf conf = new TajoConf();
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.DIRECTRAW);
    Path outputFile = writeRowBlock(conf, meta, rowBlock);
    rowBlock.free();

    DirectRawFileScanner reader = new DirectRawFileScanner(conf, schema, meta, outputFile);
    reader.init();

    int j = 0;
    while (reader.next() != null) {
      j++;
    }
    assertEquals(rowNum, j);

    for (int i = 0; i < 5; i++) {
      assertNull(reader.next());
    }

    reader.reset();

    j = 0;
    while (reader.next() != null) {
      j++;
    }
    assertEquals(rowNum, j);

    for (int i = 0; i < 5; i++) {
      assertNull(reader.next());
    }
    reader.close();
  }

  //@Test
  public void testRWWithAddTupleForAllTypes() throws IOException {
    int rowNum = 10;

    RowOrientedRowBlock rowBlock = TestRowOrientedRowBlock.createRowBlock(rowNum);

    Path testDir = CommonTestingUtil.getTestDir();
    Path outputFile = new Path(testDir, "output.draw");
    TajoConf conf = new TajoConf();
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.DIRECTRAW);
    DirectRawFileWriter writer = new DirectRawFileWriter(conf, schema, meta, outputFile);
    writer.init();

    rowBlock.resetRowCursor();
    int i = 0;
    UnSafeTuple tuple = new UnSafeTuple();
    while(rowBlock.next(tuple)) {
      writer.addTuple(tuple);
    }
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
        TestRowOrientedRowBlock.validateTupleResult(j, tuple);
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

  //@Test
  public void testNullityValidation() throws IOException {
    int rowNum = 1000;

    long allocateStart = System.currentTimeMillis();
    RowOrientedRowBlock rowBlock = new RowOrientedRowBlock(schema, 1024);
    long allocatedEnd = System.currentTimeMillis();
    LOG.info(FileUtil.humanReadableByteCount(rowBlock.totalMem(), true) + " bytes allocated "
        + (allocatedEnd - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < rowNum; i++) {
      fillRowBlockWithNull(i, rowBlock);
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
    UnSafeTuple tuple = new UnSafeTuple();
    int j = 0;

    do {
      readBlock.resetRowCursor();
      while (readBlock.next(tuple)) {
        validateNullity(j, tuple);
        j++;
      }
    } while(reader.next(readBlock));
    LOG.info("Total read rows: " + j);
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");
    reader.close();
    readBlock.free();

    assertEquals(rowNum, j);
  }
}
