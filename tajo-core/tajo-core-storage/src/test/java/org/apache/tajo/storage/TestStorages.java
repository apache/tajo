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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Options;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.rcfile.RCFile;
import org.apache.tajo.util.CommonTestingUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class TestStorages {
	private TajoConf conf;
	private static String TEST_PATH = "target/test-data/TestStorages";

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
        {StoreType.RAW, false, false},
    });
  }
		
	@Test
  public void testSplitable() throws IOException {
    if (splitable) {
      Schema schema = new Schema();
      schema.addColumn("id", Type.INT4);
      schema.addColumn("age", Type.INT8);

      TableMeta meta = CatalogUtil.newTableMeta(schema, storeType);
      Path tablePath = new Path(testDir, "Splitable.data");
      Appender appender = StorageManager.getAppender(conf, meta, tablePath);
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
      TableStat stat = appender.getStats();
      assertEquals(tupleNum, stat.getNumRows().longValue());

      FileStatus status = fs.getFileStatus(tablePath);
      long fileLen = status.getLen();
      long randomNum = (long) (Math.random() * fileLen) + 1;

      Fragment[] tablets = new Fragment[2];
      tablets[0] = new Fragment("Splitable", tablePath, meta,
          0, randomNum, null);
      tablets[1] = new Fragment("Splitable", tablePath, meta,
          randomNum, (fileLen - randomNum), null);

      Scanner scanner = StorageManager.getScanner(conf, meta, tablets[0], schema);
      scanner.init();
      int tupleCnt = 0;
      while (scanner.next() != null) {
        tupleCnt++;
      }
      scanner.close();

      scanner = StorageManager.getScanner(conf, meta, tablets[1], schema);
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

    TableMeta meta = CatalogUtil.newTableMeta(schema, storeType);

    Path tablePath = new Path(testDir, "testProjection.data");
    Appender appender = StorageManager.getAppender(conf, meta, tablePath);
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
    Fragment fragment = new Fragment("testReadAndWrite", tablePath, meta, 0, status.getLen(), null);

    Schema target = new Schema();
    target.addColumn("age", Type.INT8);
    target.addColumn("score", Type.FLOAT4);
    Scanner scanner = StorageManager.getScanner(conf, meta, fragment, target);
    scanner.init();
    int tupleCnt = 0;
    Tuple tuple;
    while ((tuple = scanner.next()) != null) {
      if (storeType == StoreType.RCFILE || storeType == StoreType.TREVNI) {
        assertNull(tuple.get(0));
      }
      assertEquals(DatumFactory.createInt8(tupleCnt + 2), tuple.getLong(1));
      assertEquals(DatumFactory.createFloat4(tupleCnt + 3), tuple.getFloat(2));
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
    schema.addColumn("col3", Type.CHAR);
    schema.addColumn("col4", Type.INT2);
    schema.addColumn("col5", Type.INT4);
    schema.addColumn("col6", Type.INT8);
    schema.addColumn("col7", Type.FLOAT4);
    schema.addColumn("col8", Type.FLOAT8);
    schema.addColumn("col9", Type.TEXT);
    schema.addColumn("col10", Type.BLOB);
    schema.addColumn("col11", Type.INET4);

    Options options = new Options();
    TableMeta meta = CatalogUtil.newTableMeta(schema, storeType, options);

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = StorageManager.getAppender(conf, meta, tablePath);
    appender.init();

    Tuple tuple = new VTuple(11);
    tuple.put(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createBit((byte) 0x99),
        DatumFactory.createChar('7'),
        DatumFactory.createInt2((short) 17),
        DatumFactory.createInt4(59),
        DatumFactory.createInt8(23l),
        DatumFactory.createFloat4(77.9f),
        DatumFactory.createFloat8(271.9f),
        DatumFactory.createText("hyunsik"),
        DatumFactory.createBlob("hyunsik".getBytes()),
        DatumFactory.createInet4("192.168.0.1"),
        DatumFactory.createText("hyunsik")
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    Fragment fragment = new Fragment("table", tablePath, meta, 0, status.getLen(), null);
    Scanner scanner =  StorageManager.getScanner(conf, meta, fragment);
    scanner.init();
    Tuple retrieved = scanner.next();
    for (int i = 0; i < tuple.size(); i++) {
      assertEquals(tuple.get(i), retrieved.get(i));
    }
  }
}
