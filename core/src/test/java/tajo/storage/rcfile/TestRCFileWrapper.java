/*
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

package tajo.storage.rcfile;

import org.apache.hadoop.fs.FileStatus;
import org.junit.Before;
import org.junit.Test;
import tajo.WorkerTestingUtil;
import tajo.catalog.Options;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.catalog.statistics.TableStat;
import tajo.conf.TajoConf;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.ipc.protocolrecords.Fragment;
import tajo.storage.*;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestRCFileWrapper {
	private TajoConf conf;
	private static String TEST_PATH = "target/test-data/RCFileWrapper";
	private StorageManager sm;
	
	@Before
	public void setUp() throws Exception {
		conf = new TajoConf();
		conf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, 100);
		WorkerTestingUtil.buildTestDir(TEST_PATH);
		sm = StorageManager.get(conf, TEST_PATH);
	}
		
	@Test
  public void testReadAndWrite() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.LONG);
    
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.RCFILE);
    
    sm.initTableBase(meta, "testReadAndWrite");
    Appender appender = sm.getAppender(meta, "testReadAndWrite", "file1");
    int tupleNum = 10000;
    VTuple vTuple;
    
    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, DatumFactory.createInt(i+1));
      vTuple.put(1, DatumFactory.createLong(25l));
      appender.addTuple(vTuple);
    }
    appender.close();
    TableStat stat = appender.getStats();
    assertEquals(tupleNum, stat.getNumRows().longValue());
    
    FileStatus status = sm.listTableFiles("testReadAndWrite")[0];
    long fileLen = status.getLen();
    long randomNum = (long) (Math.random() * fileLen) + 1;
    
    Fragment[] tablets = new Fragment[2];
    tablets[0] = new Fragment("testReadAndWrite", status.getPath(), meta,
        0, randomNum);
    tablets[1] = new Fragment("testReadAndWrite", status.getPath(), meta,
        randomNum, (fileLen - randomNum));

    Scanner scanner = new RCFileWrapper.RCFileScanner(conf, meta.getSchema(),
        tablets[0], schema);
    int tupleCnt = 0;
    while (scanner.next() != null) {
      tupleCnt++;
    }
    scanner.close();

    scanner = new RCFileWrapper.RCFileScanner(conf, meta.getSchema(),
        tablets[1], schema);
    while (scanner.next() != null) {
      tupleCnt++;
    }
    scanner.close();

    assertEquals(tupleNum, tupleCnt);
	}

  @Test
  public void testVariousTypes() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col1", DataType.BOOLEAN);
    schema.addColumn("col2", DataType.BYTE);
    schema.addColumn("col3", DataType.CHAR);
    schema.addColumn("col4", DataType.SHORT);
    schema.addColumn("col5", DataType.INT);
    schema.addColumn("col6", DataType.LONG);
    schema.addColumn("col7", DataType.FLOAT);
    schema.addColumn("col8", DataType.DOUBLE);
    schema.addColumn("col9", DataType.STRING);
    schema.addColumn("col10", DataType.BYTES);
    schema.addColumn("col11", DataType.IPv4);

    Options options = new Options();
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.RCFILE, options);

    sm.initTableBase(meta, "rcfile");
    Appender appender = sm.getAppender(meta, "rcfile", "table.dat");

    Tuple tuple = new VTuple(11);
    tuple.put(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createByte((byte) 0x99),
        DatumFactory.createChar('7'),
        DatumFactory.createShort((short) 17),
        DatumFactory.createInt(59),
        DatumFactory.createLong(23l),
        DatumFactory.createFloat(77.9f),
        DatumFactory.createDouble(271.9f),
        DatumFactory.createString("hyunsik"),
        DatumFactory.createBytes("hyunsik".getBytes()),
        DatumFactory.createIPv4("192.168.0.1")
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = sm.listTableFiles("rcfile")[0];
    Fragment fragment = new Fragment("rcfile", status.getPath(), meta,
        0, status.getLen());
    Scanner scanner =  new RCFileWrapper.RCFileScanner(conf, meta.getSchema(),
        fragment, schema);
    Tuple retrieved = scanner.next();
    for (int i = 0; i < tuple.size(); i++) {
      assertEquals(tuple.get(i), retrieved.get(i));
    }
  }
}
