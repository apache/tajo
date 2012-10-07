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

package tajo.storage;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import tajo.WorkerTestingUtil;
import tajo.catalog.Options;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf;
import tajo.datum.ArrayDatum;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestRawFile {
	private TajoConf conf;
	private static String TEST_PATH = "target/test-data/TestRawFile";

	@Before
	public void setUp() throws Exception {
		conf = new TajoConf();
		WorkerTestingUtil.buildTestDir(TEST_PATH);
	}

	@Test
  public void testRawFile() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.LONG);
    schema.addColumn("description", DataType.STRING);
    schema.addColumn("null_one", DataType.LONG);
    schema.addColumn("array", DataType.ARRAY);

    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.RAW);

    FileSystem fs = FileSystem.getLocal(conf);
    Path path = fs.makeQualified(new Path(TEST_PATH, "raw.dat"));

    RawFile.Appender appender = new RawFile.Appender(conf, meta, path);
    appender.init();
    int tupleNum = 10000;
    VTuple vTuple;
    Datum stringDatum = DatumFactory.createString("abcdefghijklmnopqrstuvwxyz");
    ArrayDatum array = new ArrayDatum( new Datum[] {
        DatumFactory.createLong(1234),
        DatumFactory.createLong(4567)
    });

    long startAppend = System.currentTimeMillis();
    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(5);
      vTuple.put(0, DatumFactory.createInt(i+1));
      vTuple.put(1, DatumFactory.createLong(25l));
      vTuple.put(2, stringDatum);
      vTuple.put(3, DatumFactory.createNullDatum());
      vTuple.put(4, array);
      appender.addTuple(vTuple);
    }
    long endAppend = System.currentTimeMillis();
    appender.close();
    
    //TableStat stat = appender.getStats();
    //assertEquals(tupleNum, stat.getNumRows().longValue());

    RawFile.Scanner scanner = new RawFile.Scanner(conf, meta, path);
    scanner.init();
    int tupleCnt = 0;
    long startScan = System.currentTimeMillis();
    while (scanner.next() != null) {
      tupleCnt++;
    }
    long endScan = System.currentTimeMillis();
    scanner.close();

    assertEquals(tupleNum, tupleCnt);

    System.out.println("Append time: " + (endAppend - startAppend) + " msc");
    System.out.println("Scan time: " + (endScan - startScan) + " msc");
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
    TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.RAW, options);

    FileSystem fs = FileSystem.getLocal(conf);
    Path path = fs.makeQualified(new Path(TEST_PATH, "raw.dat"));

    RawFile.Appender appender = new RawFile.Appender(conf, meta, path);
    appender.init();

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

    RawFile.Scanner scanner = new RawFile.Scanner(conf, meta, path);
    scanner.init();
    Tuple retrieved = scanner.next();
    for (int i = 0; i < tuple.size(); i++) {
      assertEquals(tuple.get(i), retrieved.get(i));
    }
    scanner.close();
  }
}
