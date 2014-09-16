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
import org.apache.tajo.tuple.offheap.OffHeapRowBlock;
import org.apache.tajo.tuple.offheap.OffHeapRowBlockReader;
import org.apache.tajo.tuple.offheap.TestOffHeapRowBlock;
import org.apache.tajo.tuple.offheap.ZeroCopyTuple;
import org.apache.tajo.storage.rawfile.DirectRawFileScanner;
import org.apache.tajo.storage.rawfile.DirectRawFileWriter;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tajo.tuple.offheap.TestOffHeapRowBlock.*;
import static org.junit.Assert.*;

public class TestDirectRawFile {
  private static final Log LOG = LogFactory.getLog(TestDirectRawFile.class);

  public static Path writeRowBlock(TajoConf conf, TableMeta meta, OffHeapRowBlock rowBlock, Path outputFile)
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

  public static Path writeRowBlock(TajoConf conf, TableMeta meta, OffHeapRowBlock rowBlock) throws IOException {
    Path testDir = CommonTestingUtil.getTestDir();
    Path outputFile = new Path(testDir, "output.draw");
    return writeRowBlock(conf, meta, rowBlock, outputFile);
  }

  //@Test
  public void testRWForAllTypes() throws IOException {
    int rowNum = 50000;

    OffHeapRowBlock rowBlock = TestOffHeapRowBlock.createRowBlock(rowNum);
    TajoConf conf = new TajoConf();
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.DIRECTRAW);
    Path outputFile = writeRowBlock(conf, meta, rowBlock);
    rowBlock.release();


    FileSystem fs = FileSystem.getLocal(conf);
    FileStatus status = fs.getFileStatus(outputFile);
    assertTrue(status.getLen() > 0);
    LOG.info("Written file size: " + FileUtil.humanReadableByteCount(status.getLen(), false));


    OffHeapRowBlock readBlock = new OffHeapRowBlock(schema, StorageUnit.MB * 1);
    OffHeapRowBlockReader blockReader = new OffHeapRowBlockReader(readBlock);
    DirectRawFileScanner reader = new DirectRawFileScanner(conf, schema, meta, outputFile);
    reader.init();

    long readStart = System.currentTimeMillis();
    ZeroCopyTuple tuple = new ZeroCopyTuple();
    int j = 0;

    while(reader.next(readBlock)) {
      blockReader.reset();
      while (blockReader.next(tuple)) {
        TestOffHeapRowBlock.validateTupleResult(j, tuple);
        j++;
      }
    }
    LOG.info("Total read rows: " + j);
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");
    reader.close();
    readBlock.release();

    assertEquals(rowNum, j);
  }

  @Test
  public void testRWForAllTypesWithNextTuple() throws IOException {
    int rowNum = 10000;

    OffHeapRowBlock rowBlock = TestOffHeapRowBlock.createRowBlock(rowNum);

    TajoConf conf = new TajoConf();
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.DIRECTRAW);
    Path outputFile = writeRowBlock(conf, meta, rowBlock);
    rowBlock.release();

    DirectRawFileScanner reader = new DirectRawFileScanner(conf, schema, meta, outputFile);
    reader.init();

    long readStart = System.currentTimeMillis();
    int j = 0;
    Tuple tuple;
    while ((tuple = reader.next()) != null) {
      TestOffHeapRowBlock.validateTupleResult(j, tuple);
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

    OffHeapRowBlock rowBlock = TestOffHeapRowBlock.createRowBlock(rowNum);

    TajoConf conf = new TajoConf();
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.DIRECTRAW);
    Path outputFile = writeRowBlock(conf, meta, rowBlock);

    rowBlock.release();

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

    OffHeapRowBlock rowBlock = TestOffHeapRowBlock.createRowBlock(rowNum);

    TajoConf conf = new TajoConf();
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.DIRECTRAW);
    Path outputFile = writeRowBlock(conf, meta, rowBlock);
    rowBlock.release();

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

    OffHeapRowBlock rowBlock = TestOffHeapRowBlock.createRowBlock(rowNum);
    OffHeapRowBlockReader blockReader = new OffHeapRowBlockReader(rowBlock);

    Path testDir = CommonTestingUtil.getTestDir();
    Path outputFile = new Path(testDir, "output.draw");
    TajoConf conf = new TajoConf();
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.DIRECTRAW);
    DirectRawFileWriter writer = new DirectRawFileWriter(conf, schema, meta, outputFile);
    writer.init();

    blockReader.reset();
    int i = 0;
    ZeroCopyTuple tuple = new ZeroCopyTuple();
    while(blockReader.next(tuple)) {
      writer.addTuple(tuple);
    }
    writer.close();
    rowBlock.release();


    FileSystem fs = FileSystem.getLocal(conf);
    FileStatus status = fs.getFileStatus(outputFile);
    assertTrue(status.getLen() > 0);
    LOG.info("Written file size: " + FileUtil.humanReadableByteCount(status.getLen(), false));

    OffHeapRowBlock readBlock = new OffHeapRowBlock(schema, StorageUnit.MB * 1);
    OffHeapRowBlockReader blockReader2 = new OffHeapRowBlockReader(readBlock);
    DirectRawFileScanner reader = new DirectRawFileScanner(conf, schema, meta, outputFile);
    reader.init();

    long readStart = System.currentTimeMillis();
    tuple = new ZeroCopyTuple();
    int j = 0;
    while(reader.next(readBlock)) {
      blockReader2.reset();
      while (blockReader2.next(tuple)) {
        TestOffHeapRowBlock.validateTupleResult(j, tuple);
        j++;
      }
    }
    LOG.info("Total read rows: " + j);
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");
    reader.close();
    readBlock.release();

    assertEquals(rowNum, j);
  }

  //@Test
  public void testNullityValidation() throws IOException {
    int rowNum = 1000;

    long allocateStart = System.currentTimeMillis();
    OffHeapRowBlock rowBlock = new OffHeapRowBlock(schema, 1024);
    long allocatedEnd = System.currentTimeMillis();
    LOG.info(FileUtil.humanReadableByteCount(rowBlock.size(), true) + " bytes allocated "
        + (allocatedEnd - allocateStart) + " msec");

    long writeStart = System.currentTimeMillis();
    for (int i = 0; i < rowNum; i++) {
      fillRowBlockWithNull(i, rowBlock.getWriter());
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
    rowBlock.release();


    FileSystem fs = FileSystem.getLocal(conf);
    FileStatus status = fs.getFileStatus(outputFile);
    assertTrue(status.getLen() > 0);
    LOG.info("Written file size: " + FileUtil.humanReadableByteCount(status.getLen(), false));

    OffHeapRowBlock readBlock = new OffHeapRowBlock(schema, StorageUnit.MB * 1);
    OffHeapRowBlockReader blockReader = new OffHeapRowBlockReader(rowBlock);
    DirectRawFileScanner reader = new DirectRawFileScanner(conf, schema, meta, outputFile);
    reader.init();

    long readStart = System.currentTimeMillis();
    ZeroCopyTuple tuple = new ZeroCopyTuple();
    int j = 0;

    do {
      blockReader.reset();
      while (blockReader.next(tuple)) {
        validateNullity(j, tuple);
        j++;
      }
    } while(reader.next(readBlock));
    LOG.info("Total read rows: " + j);
    long readEnd = System.currentTimeMillis();
    LOG.info("reading takes " + (readEnd - readStart) + " msec");
    reader.close();
    readBlock.release();

    assertEquals(rowNum, j);
  }
}
