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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.util.CommonTestingUtil;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestStorageManager {
	private TajoConf conf;
	private static String TEST_PATH = "target/test-data/TestStorageManager";
	StorageManager sm = null;
  private Path testDir;
  private FileSystem fs;
	@Before
	public void setUp() throws Exception {
		conf = new TajoConf();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
    sm = StorageManager.get(conf, testDir);
	}

	@After
	public void tearDown() throws Exception {
	}

  @Test
	public final void testGetScannerAndAppender() throws IOException {
		Schema schema = new Schema();
		schema.addColumn("id", Type.INT4);
		schema.addColumn("age",Type.INT4);
		schema.addColumn("name",Type.TEXT);

		TableMeta meta = CatalogUtil.newTableMeta(schema, StoreType.CSV);
		
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
		Appender appender = StorageManager.getAppender(conf, meta, path);
    appender.init();
		for(Tuple t : tuples) {
		  appender.addTuple(t);
		}
		appender.close();

		Scanner scanner = StorageManager.getScanner(conf, meta, path);
    scanner.init();
		int i=0;
		while(scanner.next() != null) {
			i++;
		}
		assertEquals(4,i);
	}
}
