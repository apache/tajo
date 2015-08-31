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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.rawfile.DirectRawFileScanner;
import org.apache.tajo.storage.rawfile.DirectRawFileWriter;
import org.apache.tajo.tuple.memory.MemoryRowBlock;
import org.apache.tajo.tuple.memory.RowWriter;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.ProtoUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestDirectRawFile {
  private static final Log LOG = LogFactory.getLog(TestDirectRawFile.class);
  public static String UNICODE_FIELD_PREFIX = "abc_가나다_";
  public static Schema schema;

  private static String TEST_PATH = "target/test-data/TestDirectRawFile";
  private static MiniDFSCluster cluster;
  private static FileSystem dfs;
  private static FileSystem localFs;

  private TajoConf tajoConf;
  private Path testDir;

  @Parameterized.Parameters
  public static Collection<Object[]> generateParameters() throws IOException {
    return Arrays.asList(new Object[][]{
        {false},
        {true}
    });
  }


  public TestDirectRawFile(boolean isLocal) throws IOException {
    FileSystem fs;
    if (isLocal) {
      fs = localFs;
    } else {
      fs = dfs;
    }

    this.tajoConf = new TajoConf(fs.getConf());
    this.testDir = getTestDir(fs, TEST_PATH);
  }

  @BeforeClass
  public static void setUpClass() throws IOException, InterruptedException {
    final Configuration conf = new HdfsConfiguration();
    String testDataPath = TEST_PATH + "/" + UUID.randomUUID().toString();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDataPath);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    conf.setBoolean(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED, false);

    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(new HdfsConfiguration(conf));
    builder.numDataNodes(1);
    builder.format(true);
    builder.manageNameDfsDirs(true);
    builder.manageDataDfsDirs(true);
    builder.waitSafeMode(true);
    cluster = builder.build();

    cluster.waitClusterUp();
    dfs = cluster.getFileSystem();
    localFs = FileSystem.getLocal(new TajoConf());
  }

  @AfterClass
  public static void tearDownClass() throws InterruptedException {
    cluster.shutdown(true);
  }

  public Path getTestDir(FileSystem fs, String dir) throws IOException {
    Path path = new Path(dir);
    if(fs.exists(path))
      fs.delete(path, true);

    fs.mkdirs(path);

    return fs.makeQualified(path);
  }

  static {
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
    schema.addColumn("col10", TajoDataTypes.Type.INTERVAL);
    schema.addColumn("col11", TajoDataTypes.Type.INET4);
    schema.addColumn("col12",
        CatalogUtil.newDataType(TajoDataTypes.Type.PROTOBUF, PrimitiveProtos.StringProto.class.getName()));
  }

  public FileStatus writeRowBlock(TajoConf conf, TableMeta meta, MemoryRowBlock rowBlock, Path outputFile)
      throws IOException {
    DirectRawFileWriter writer = new DirectRawFileWriter(conf, null, schema, meta, outputFile);
    writer.init();
    writer.writeRowBlock(rowBlock);
    writer.close();

    FileStatus status = outputFile.getFileSystem(conf).getFileStatus(outputFile);
    assertTrue(status.getLen() > 0);
    LOG.info("Written file size: " + FileUtil.humanReadableByteCount(status.getLen(), false));
    return status;
  }

  public FileStatus writeRowBlock(TajoConf conf, TableMeta meta, MemoryRowBlock rowBlock) throws IOException {
    Path outputDir = new Path(testDir, UUID.randomUUID() + "");
    outputDir.getFileSystem(conf).mkdirs(outputDir);
    Path outputFile = new Path(outputDir, "output.draw");
    return writeRowBlock(conf, meta, rowBlock, outputFile);
  }

  @Test
  public void testRWForAllTypesWithNextTuple() throws IOException {
    int rowNum = 10000;

    MemoryRowBlock rowBlock = createRowBlock(rowNum);

    TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.DRAW);
    FileStatus outputFile = writeRowBlock(tajoConf, meta, rowBlock);
    rowBlock.release();

    FileFragment fragment =
        new FileFragment("testRWForAllTypesWithNextTuple", outputFile.getPath(), 0, outputFile.getLen());
    DirectRawFileScanner reader = new DirectRawFileScanner(tajoConf, schema, meta, fragment);
    reader.init();

    long readStart = System.currentTimeMillis();
    int j = 0;
    Tuple tuple;
    while ((tuple = reader.next()) != null) {
      validateTupleResult(j, tuple);
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

    MemoryRowBlock rowBlock = createRowBlock(rowNum);
    TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.DRAW);
    FileStatus outputFile = writeRowBlock(tajoConf, meta, rowBlock);

    rowBlock.release();

    FileFragment fragment =
        new FileFragment("testRepeatedScan", outputFile.getPath(), 0, outputFile.getLen());
    DirectRawFileScanner reader = new DirectRawFileScanner(tajoConf, schema, meta, fragment);
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

    MemoryRowBlock rowBlock = createRowBlock(rowNum);

    TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.DRAW);
    FileStatus outputFile = writeRowBlock(tajoConf, meta, rowBlock);
    rowBlock.release();

    FileFragment fragment =
        new FileFragment("testReset", outputFile.getPath(), 0, outputFile.getLen());
    DirectRawFileScanner reader = new DirectRawFileScanner(tajoConf, schema, meta, fragment);
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

  public static MemoryRowBlock createRowBlock(int rowNum) {
    long allocateStart = System.currentTimeMillis();
    MemoryRowBlock rowBlock = new MemoryRowBlock(SchemaUtil.toDataTypes(schema), StorageUnit.KB * 128);
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

  public static void fillRow(int i, RowWriter builder) {
    builder.startRow();
    builder.putBool(i % 1 == 0 ? true : false); // 0
    builder.putInt2((short) 1);                 // 1
    builder.putInt4(i);                         // 2
    builder.putInt8(i);                         // 3
    builder.putFloat4(i);                       // 4
    builder.putFloat8(i);                       // 5
    builder.putText((UNICODE_FIELD_PREFIX + i).getBytes());  // 6
    builder.putTimestamp(DatumFactory.createTimestamp("2014-04-16 08:48:00").asInt8() + i); // 7
    builder.putDate(DatumFactory.createDate("2014-04-16").asInt4() + i); // 8
    builder.putTime(DatumFactory.createTime("08:48:00").asInt8() + i); // 9
    builder.putInterval(DatumFactory.createInterval((i + 1) + " hours")); // 10
    builder.putInet4(DatumFactory.createInet4("192.168.0.1").asInt4() + i); // 11
    builder.putProtoDatum(new ProtobufDatum(ProtoUtil.convertString(i + ""))); // 12
    builder.endRow();
  }

  public static void validateTupleResult(int j, Tuple t) {
    assertTrue((j % 1 == 0) == t.getBool(0));
    assertTrue(1 == t.getInt2(1));
    assertEquals(j, t.getInt4(2));
    assertEquals(j, t.getInt8(3));
    assertTrue(j == t.getFloat4(4));
    assertTrue(j == t.getFloat8(5));
    assertEquals(new String(UNICODE_FIELD_PREFIX + j), t.getText(6));
    assertEquals(DatumFactory.createTimestamp("2014-04-16 08:48:00").asInt8() + (long) j, t.getInt8(7));
    assertEquals(DatumFactory.createDate("2014-04-16").asInt4() + j, t.getInt4(8));
    assertEquals(DatumFactory.createTime("08:48:00").asInt8() + j, t.getInt8(9));
    assertEquals(DatumFactory.createInterval((j + 1) + " hours"), t.getInterval(10));
    assertEquals(DatumFactory.createInet4("192.168.0.1").asInt4() + j, t.getInt4(11));
    assertEquals(new ProtobufDatum(ProtoUtil.convertString(j + "")), t.getProtobufDatum(12));
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
      writer.putText((UNICODE_FIELD_PREFIX + i).getBytes());  // 6
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
      assertEquals(new String(UNICODE_FIELD_PREFIX + j), tuple.getText(6));
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
