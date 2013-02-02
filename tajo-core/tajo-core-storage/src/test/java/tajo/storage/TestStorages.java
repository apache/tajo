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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
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
import tajo.storage.rcfile.RCFile;
import tajo.util.CommonTestingUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

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
      schema.addColumn("id", DataType.INT);
      schema.addColumn("age", DataType.LONG);

      TableMeta meta = TCatUtil.newTableMeta(schema, storeType);
      Path tablePath = new Path(testDir, "Splitable.data");
      Appender appender = StorageManager.getAppender(conf, meta, tablePath);
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

      FileStatus status = fs.getFileStatus(tablePath);
      long fileLen = status.getLen();
      long randomNum = (long) (Math.random() * fileLen) + 1;

      Fragment[] tablets = new Fragment[2];
      tablets[0] = new Fragment("Splitable", tablePath, meta,
          0, randomNum, null);
      tablets[1] = new Fragment("Splitable", tablePath, meta,
          randomNum, (fileLen - randomNum), null);

      Scanner scanner = StorageManager.getScanner(conf, meta, tablets[0], schema);
      int tupleCnt = 0;
      while (scanner.next() != null) {
        tupleCnt++;
      }
      scanner.close();

      scanner = StorageManager.getScanner(conf, meta, tablets[1], schema);
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
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.LONG);
    schema.addColumn("score", DataType.FLOAT);

    TableMeta meta = TCatUtil.newTableMeta(schema, storeType);

    Path tablePath = new Path(testDir, "testProjection.data");
    Appender appender = StorageManager.getAppender(conf, meta, tablePath);
    int tupleNum = 10000;
    VTuple vTuple;

    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(3);
      vTuple.put(0, DatumFactory.createInt(i+1));
      vTuple.put(1, DatumFactory.createLong(i+2));
      vTuple.put(2, DatumFactory.createFloat(i + 3));
      appender.addTuple(vTuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    Fragment fragment = new Fragment("testReadAndWrite", tablePath, meta, 0, status.getLen(), null);

    Schema target = new Schema();
    target.addColumn("age", DataType.LONG);
    target.addColumn("score", DataType.FLOAT);
    Scanner scanner = StorageManager.getScanner(conf, meta, fragment, target);
    int tupleCnt = 0;
    Tuple tuple;
    while ((tuple = scanner.next()) != null) {
      assertEquals(DatumFactory.createLong(tupleCnt + 2), tuple.getLong(1));
      assertEquals(DatumFactory.createFloat(tupleCnt + 3), tuple.getFloat(2));
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
    schema.addColumn("col12", DataType.STRING2);

    Options options = new Options();
    TableMeta meta = TCatUtil.newTableMeta(schema, storeType, options);

    Path tablePath = new Path(testDir, "testVariousTypes.data");
    Appender appender = StorageManager.getAppender(conf, meta, tablePath);

    Tuple tuple = new VTuple(12);
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
        DatumFactory.createIPv4("192.168.0.1"),
        DatumFactory.createString2("hyunsik")
    });
    appender.addTuple(tuple);
    appender.flush();
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    Fragment fragment = new Fragment("table", tablePath, meta, 0, status.getLen(), null);
    Scanner scanner =  StorageManager.getScanner(conf, meta, fragment);
    Tuple retrieved = scanner.next();
    for (int i = 0; i < tuple.size(); i++) {
      assertEquals(tuple.get(i), retrieved.get(i));
    }
  }
}
