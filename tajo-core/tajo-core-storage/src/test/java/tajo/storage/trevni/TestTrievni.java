/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.storage.trevni;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
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
import tajo.storage.*;
import tajo.storage.rcfile.RCFile;
import tajo.util.CommonTestingUtil;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestTrievni {
	private TajoConf conf;
	private static String TEST_PATH = "target/test-data/TestTrievni";
	private StorageManager sm;
	
	@Before
	public void setUp() throws Exception {
		conf = new TajoConf();
		conf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, 100);
		Path createdDir = CommonTestingUtil.buildTestDir(TEST_PATH);
		sm = StorageManager.get(conf, createdDir);
	}
		
	@Test
  public void testReadAndWrite() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.LONG);
    
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.TREVNI);
    
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
    
    Fragment fragment = new Fragment("testReadAndWrite", status.getPath(), meta, 0, fileLen, null);


    Scanner scanner = new TrevniScanner(conf, meta.getSchema(), fragment, meta.getSchema());
    int tupleCnt = 0;
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
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.TREVNI, options);

    sm.initTableBase(meta, "trevni");
    Appender appender = sm.getAppender(meta, "trevni", "table.trv");

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

    FileStatus status = sm.listTableFiles("trevni")[0];
    Fragment fragment = new Fragment("trevni", status.getPath(), meta,
        0, status.getLen(), null);
    Scanner scanner =  new TrevniScanner(conf, meta.getSchema(),
        fragment, schema);
    Tuple retrieved = scanner.next();
    for (int i = 0; i < tuple.size(); i++) {
      assertEquals(tuple.get(i), retrieved.get(i));
    }
  }
}
