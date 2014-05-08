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

package org.apache.tajo.storage.v2;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.rcfile.RCFile;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.tajo.conf.TajoConf.ConfVars;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestStorages {
	private TajoConf conf;
	private static String TEST_PATH = "target/test-data/v2/TestStorages";

  private static String TEST_PROJECTION_AVRO_SCHEMA =
      "{\n" +
      "  \"type\": \"record\",\n" +
      "  \"namespace\": \"org.apache.tajo\",\n" +
      "  \"name\": \"testProjection\",\n" +
      "  \"fields\": [\n" +
      "    { \"name\": \"id\", \"type\": \"int\" },\n" +
      "    { \"name\": \"age\", \"type\": \"long\" },\n" +
      "    { \"name\": \"score\", \"type\": \"float\" }\n" +
      "  ]\n" +
      "}\n";

  private static String TEST_VARIOUS_TYPES_AVRO_SCHEMA =
      "{\n" +
      "  \"type\": \"record\",\n" +
      "  \"namespace\": \"org.apache.tajo\",\n" +
      "  \"name\": \"testVariousTypes\",\n" +
      "  \"fields\": [\n" +
      "    { \"name\": \"col1\", \"type\": \"boolean\" },\n" +
      "    { \"name\": \"col2\", \"type\": \"int\" },\n" +
      "    { \"name\": \"col3\", \"type\": \"string\" },\n" +
      "    { \"name\": \"col4\", \"type\": \"int\" },\n" +
      "    { \"name\": \"col5\", \"type\": \"int\" },\n" +
      "    { \"name\": \"col6\", \"type\": \"long\" },\n" +
      "    { \"name\": \"col7\", \"type\": \"float\" },\n" +
      "    { \"name\": \"col8\", \"type\": \"double\" },\n" +
      "    { \"name\": \"col9\", \"type\": \"string\" },\n" +
      "    { \"name\": \"col10\", \"type\": \"bytes\" },\n" +
      "    { \"name\": \"col11\", \"type\": \"bytes\" },\n" +
      "    { \"name\": \"col12\", \"type\": \"null\" }\n" +
      "  ]\n" +
      "}\n";

  private StoreType storeType;
  private boolean splitable;
  private boolean statsable;
  private Path testDir;
  private FileSystem fs;

  public TestStorages(StoreType type, boolean splitable, boolean statsable) throws IOException {
    this.storeType = type;
    this.splitable = splitable;
    this.statsable = statsable;

    conf = new TajoConf();
    conf.setBoolVar(ConfVars.STORAGE_MANAGER_VERSION_2, true);

    if (storeType == StoreType.RCFILE) {
      conf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, 100);
    }


    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][]{
        {StoreType.CSV, true, true},
        {StoreType.RCFILE, true, true},
        {StoreType.TREVNI, false, true},
        {StoreType.PARQUET, false, false},
        {StoreType.AVRO, false, false},
        {StoreType.RAW, false, false},
    });
  }

	@Test
  public void testSplitable() throws IOException {
    if (splitable) {
      Schema schema = new Schema();
      schema.addColumn("id", Type.INT4);
      schema.addColumn("age", Type.INT8);

      TableMeta meta = CatalogUtil.newTableMeta(storeType);
      meta.setOptions(StorageUtil.newPhysicalProperties(storeType));
      Path tablePath = new Path(testDir, "Splitable.data");
      Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
      appender.enableStats();
      appender.init();
      int tupleNum = 10000;
      VTuple vTuple;

      for(int i = 0; i < tupleNum; i++) {
        vTuple = new VTuple(2);
        vTuple.put(0, DatumFactory.createInt4(i + 1));
        vTuple.put(1, DatumFactory.createInt8(25l));
        appender.addTuple(vTuple);
      }
      appender.close();
      TableStats stat = appender.getStats();
      assertEquals(tupleNum, stat.getNumRows().longValue());

      FileStatus status = fs.getFileStatus(tablePath);
      long fileLen = status.getLen();
      long randomNum = (long) (Math.random() * fileLen) + 1;

      FileFragment[] tablets = new FileFragment[2];
      tablets[0] = new FileFragment("Splitable", tablePath, 0, randomNum);
      tablets[1] = new FileFragment("Splitable", tablePath, randomNum, (fileLen - randomNum));

      Scanner scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, tablets[0], schema);
      scanner.init();
      int tupleCnt = 0;
      while (scanner.next() != null) {
        tupleCnt++;
      }
      scanner.close();

      scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, tablets[1], schema);
      scanner.init();
      while (scanner.next() != null) {
        tupleCnt++;
      }
      scanner.close();

      assertEquals(tupleNum, tupleCnt);
    }
	}

  @Test
  public void testProjection() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("age", Type.INT8);
    schema.addColumn("score", Type.FLOAT4);

    TableMeta meta = CatalogUtil.newTableMeta(storeType);
    meta.setOptions(StorageUtil.newPhysicalProperties(storeType));
    if (storeType == StoreType.AVRO) {
      meta.putOption(StorageConstants.AVRO_SCHEMA_LITERAL,
                     TEST_PROJECTION_AVRO_SCHEMA);
    }

    Path tablePath = new Path(testDir, "testProjection.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.init();
    int tupleNum = 10000;
    VTuple vTuple;

    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(3);
      vTuple.put(0, DatumFactory.createInt4(i + 1));
      vTuple.put(1, DatumFactory.createInt8(i + 2));
      vTuple.put(2, DatumFactory.createFloat4(i + 3));
      appender.addTuple(vTuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("testReadAndWrite", tablePath, 0, status.getLen());

    Schema target = new Schema();
    target.addColumn("age", Type.INT8);
    target.addColumn("score", Type.FLOAT4);
    Scanner scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment, target);
    scanner.init();
    int tupleCnt = 0;
    Tuple tuple;
    while ((tuple = scanner.next()) != null) {
      if (storeType == StoreType.RCFILE
          || storeType == StoreType.TREVNI
          || storeType == StoreType.CSV
          || storeType == StoreType.PARQUET
          || storeType == StoreType.AVRO) {
        assertTrue(tuple.get(0) == null || tuple.get(0) instanceof NullDatum);
      }
      assertTrue(tupleCnt + 2 == tuple.get(1).asInt8());
      assertTrue(tupleCnt + 3 == tuple.get(2).asFloat4());
      tupleCnt++;
    }
    scanner.close();

    assertEquals(tupleNum, tupleCnt);
  }

  @Test
  public void testVariousTypes() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col1", Type.BOOLEAN);
    schema.addColumn("col2", Type.BIT);
    schema.addColumn("col3", Type.CHAR, 7);
    schema.addColumn("col4", Type.INT2);
    schema.addColumn("col5", Type.INT4);
    schema.addColumn("col6", Type.INT8);
    schema.addColumn("col7", Type.FLOAT4);
    schema.addColumn("col8", Type.FLOAT8);
    schema.addColumn("col9", Type.TEXT);
    schema.addColumn("col10", Type.BLOB);
    schema.addColumn("col11", Type.INET4);
    schema.addColumn("col12", Type.NULL_TYPE);

    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.setOptions(StorageUtil.newPhysicalProperties(storeType));
    if (storeType == StoreType.AVRO) {
      meta.putOption(StorageConstants.AVRO_SCHEMA_LITERAL,
                     TEST_VARIOUS_TYPES_AVRO_SCHEMA);
    }

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.init();

    Tuple tuple = new VTuple(12);
    tuple.put(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createBit((byte) 0x99),
        DatumFactory.createChar("hyunsik"),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23l),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("hyunsik"),
        DatumFactory.createBlob("hyunsik".getBytes()),
        DatumFactory.createInet4("192.168.0.1"),
        NullDatum.get()
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("table", tablePath, 0, status.getLen());
    Scanner scanner =  StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment);
    scanner.init();

    Tuple retrieved;
    while ((retrieved=scanner.next()) != null) {
      for (int i = 0; i < tuple.size(); i++) {
        assertEquals(tuple.get(i), retrieved.get(i));
      }
    }
  }
}
