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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.*;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tajo.conf.TajoConf.ConfVars;
import static org.junit.Assert.assertEquals;

public class TestCSVScanner {
  private TajoConf conf;
  private static String TEST_PATH = "target/test-data/v2/TestCSVScanner";
  AbstractStorageManager sm = null;
  private Path testDir;
  private FileSystem fs;

  @Before
  public void setUp() throws Exception {
    conf = new TajoConf();
    conf.setBoolVar(ConfVars.STORAGE_MANAGER_VERSION_2, true);
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
    sm = StorageManagerFactory.getStorageManager(conf, testDir);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public final void testGetScannerAndAppender() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("age", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);

    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV);

    Tuple[] tuples = new Tuple[4];
    for(int i=0; i < tuples.length; i++) {
      tuples[i] = new VTuple(3);
      tuples[i].put(new Datum[] {
          DatumFactory.createInt4(i),
          DatumFactory.createInt4(i + 32),
          DatumFactory.createText("name" + i)});
    }

    Path path = StorageUtil.concatPath(testDir, "testGetScannerAndAppender", "table.csv");
    fs.mkdirs(path.getParent());
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, path);
    appender.init();
    for(Tuple t : tuples) {
      appender.addTuple(t);
    }
    appender.close();

    Scanner scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, path);
    scanner.init();
    int i=0;
    Tuple tuple = null;
    while( (tuple = scanner.next()) != null) {
      i++;
    }
    assertEquals(4,i);
  }

  @Test
  public final void testPartitionFile() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("key", TajoDataTypes.Type.TEXT);
    schema.addColumn("age", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);

    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV);


    Path path = StorageUtil.concatPath(testDir, "testPartitionFile", "table.csv");
    fs.mkdirs(path.getParent());
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, schema, path);
    appender.init();

    String keyValue = "";
    for(int i = 0; i < 100; i++) {
      keyValue += "0123456789";
    }
    keyValue = "key_" + keyValue + "_";

    String nameValue = "";
    for(int i = 0; i < 100; i++) {
      nameValue += "0123456789";
    }
    nameValue = "name_" + nameValue + "_";

    int numTuples = 100000;
    for(int i = 0; i < numTuples; i++) {
      Tuple tuple = new VTuple(3);
      tuple.put(new Datum[] {
          DatumFactory.createText(keyValue + i),
          DatumFactory.createInt4(i + 32),
          DatumFactory.createText(nameValue + i)});
      appender.addTuple(tuple);
    }
    appender.close();

    long fileLength = fs.getLength(path);
    long totalTupleCount = 0;

    int scanCount = 0;
    Tuple startTuple = null;
    Tuple lastTuple = null;
    while(true) {
      long startOffset = (64 * 1024 * 1024) * scanCount;
      long length = Math.min(64 * 1024 * 1024, fileLength - startOffset);

      Fragment fragment = new Fragment("Test", path, startOffset, length, null, null);
      Scanner scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, schema, fragment, schema);
      scanner.init();
      Tuple tuple = null;
      while( (tuple = scanner.next()) != null) {
        if(startTuple == null) {
          startTuple = tuple;
        }
        lastTuple = tuple;
        totalTupleCount++;
      }
      scanCount++;
      if(length < 64 * 1024 * 1024) {
        break;
      }
    }
    assertEquals(numTuples, totalTupleCount);
    assertEquals(keyValue + 0, startTuple.get(0).toString());
    assertEquals(keyValue + (numTuples - 1), lastTuple.get(0).toString());
  }
}
