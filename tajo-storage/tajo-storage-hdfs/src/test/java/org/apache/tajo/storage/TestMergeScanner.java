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

package org.apache.tajo.storage;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestMergeScanner {
  private TajoConf conf;
  StorageManager sm;
  private static String TEST_PATH = "target/test-data/TestMergeScanner";

  private static String TEST_MULTIPLE_FILES_AVRO_SCHEMA =
      "{\n" +
      "  \"type\": \"record\",\n" +
      "  \"namespace\": \"org.apache.tajo\",\n" +
      "  \"name\": \"testMultipleFiles\",\n" +
      "  \"fields\": [\n" +
      "    { \"name\": \"id\", \"type\": \"int\" },\n" +
      "    { \"name\": \"file\", \"type\": \"string\" },\n" +
      "    { \"name\": \"name\", \"type\": \"string\" },\n" +
      "    { \"name\": \"age\", \"type\": \"long\" }\n" +
      "  ]\n" +
      "}\n";

  private Path testDir;
  private StoreType storeType;
  private FileSystem fs;

  public TestMergeScanner(StoreType storeType) {
    this.storeType = storeType;
  }

  @Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][] {
        {StoreType.CSV},
        {StoreType.RAW},
        {StoreType.RCFILE},
        {StoreType.PARQUET},
        {StoreType.SEQUENCEFILE},
        {StoreType.AVRO},
        // RowFile requires Byte-buffer read support, so we omitted RowFile.
        //{StoreType.ROWFILE},
    });
  }

  @Before
  public void setup() throws Exception {
    conf = new TajoConf();
    conf.setVar(ConfVars.ROOT_DIR, TEST_PATH);
    conf.setStrings("tajo.storage.projectable-scanner", "rcfile", "parquet", "avro");
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
    sm = StorageManager.getFileStorageManager(conf);
  }

  @Test
  public void testMultipleFiles() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("file", Type.TEXT);
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("age", Type.INT8);

    KeyValueSet options = new KeyValueSet();
    TableMeta meta = CatalogUtil.newTableMeta(storeType, options);
    meta.setOptions(CatalogUtil.newPhysicalProperties(storeType));
    if (storeType == StoreType.AVRO) {
      meta.putOption(StorageConstants.AVRO_SCHEMA_LITERAL,
                     TEST_MULTIPLE_FILES_AVRO_SCHEMA);
    }

    Path table1Path = new Path(testDir, storeType + "_1.data");
    Appender appender1 = StorageManager.getFileStorageManager(conf).getAppender(null, null, meta, schema, table1Path);
    appender1.enableStats();
    appender1.init();
    int tupleNum = 10000;
    VTuple vTuple;

    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(4);
      vTuple.put(0, DatumFactory.createInt4(i + 1));
      vTuple.put(1, DatumFactory.createText("hyunsik"));
      vTuple.put(2, DatumFactory.createText("jihoon"));
      vTuple.put(3, DatumFactory.createInt8(25l));
      appender1.addTuple(vTuple);
    }
    appender1.close();

    TableStats stat1 = appender1.getStats();
    if (stat1 != null) {
      assertEquals(tupleNum, stat1.getNumRows().longValue());
    }

    Path table2Path = new Path(testDir, storeType + "_2.data");
    Appender appender2 = StorageManager.getFileStorageManager(conf).getAppender(null, null, meta, schema, table2Path);
    appender2.enableStats();
    appender2.init();

    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(4);
      vTuple.put(0, DatumFactory.createInt4(i + 1));
      vTuple.put(1, DatumFactory.createText("hyunsik"));
      vTuple.put(2, DatumFactory.createText("jihoon"));
      vTuple.put(3, DatumFactory.createInt8(25l));
      appender2.addTuple(vTuple);
    }
    appender2.close();

    TableStats stat2 = appender2.getStats();
    if (stat2 != null) {
      assertEquals(tupleNum, stat2.getNumRows().longValue());
    }


    FileStatus status1 = fs.getFileStatus(table1Path);
    FileStatus status2 = fs.getFileStatus(table2Path);
    Fragment[] fragment = new Fragment[2];
    fragment[0] = new FileFragment("tablet1", table1Path, 0, status1.getLen());
    fragment[1] = new FileFragment("tablet1", table2Path, 0, status2.getLen());

    Schema targetSchema = new Schema();
    targetSchema.addColumn(schema.getColumn(0));
    targetSchema.addColumn(schema.getColumn(2));

    Scanner scanner = new MergeScanner(conf, schema, meta, TUtil.newList(fragment), targetSchema);
    assertEquals(isProjectableStorage(meta.getStoreType()), scanner.isProjectable());

    scanner.init();
    int totalCounts = 0;
    Tuple tuple;
    while ((tuple = scanner.next()) != null) {
      totalCounts++;
      if (isProjectableStorage(meta.getStoreType())) {
        assertNotNull(tuple.get(0));
        assertNull(tuple.get(1));
        assertNotNull(tuple.get(2));
        assertNull(tuple.get(3));
      }
    }
    scanner.close();

    assertEquals(tupleNum * 2, totalCounts);
	}

  private static boolean isProjectableStorage(StoreType type) {
    switch (type) {
      case RCFILE:
      case PARQUET:
      case SEQUENCEFILE:
      case CSV:
      case AVRO:
        return true;
      default:
        return false;
    }
  }
}
