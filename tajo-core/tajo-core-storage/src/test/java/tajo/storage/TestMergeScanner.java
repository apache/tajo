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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import tajo.catalog.Options;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.catalog.statistics.TableStat;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.datum.DatumFactory;
import tajo.util.CommonTestingUtil;
import tajo.util.TUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestMergeScanner {
  private TajoConf conf;
  StorageManager sm;
  private static String TEST_PATH = "target/test-data/TestMergeScanner";
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
        {StoreType.TREVNI},
        // RowFile requires Byte-buffer read support, so we omitted RowFile.
        //{StoreType.ROWFILE},

    });
  }

  @Before
  public void setup() throws Exception {
    conf = new TajoConf();
    conf.setVar(ConfVars.ENGINE_DATA_DIR, TEST_PATH);
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
    sm = StorageManager.get(conf, testDir);
  }
  
  @Test
  public void testMultipleFiles() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("file", DataType.STRING);
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("age", DataType.LONG);
    
    Options options = new Options();
    TableMeta meta = TCatUtil.newTableMeta(schema, storeType, options);

    Path table1Path = new Path(testDir, storeType + "_1.data");
    Appender appender1 = StorageManager.getAppender(conf, meta, table1Path);
    int tupleNum = 10000;
    VTuple vTuple;

    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(4);
      vTuple.put(0, DatumFactory.createInt(i+1));
      vTuple.put(1, DatumFactory.createString("hyunsik"));
      vTuple.put(2, DatumFactory.createString("jihoon"));
      vTuple.put(3, DatumFactory.createLong(25l));
      appender1.addTuple(vTuple);
    }
    appender1.close();
    
    TableStat stat1 = appender1.getStats();
    if (stat1 != null) {
      assertEquals(tupleNum, stat1.getNumRows().longValue());
    }

    Path table2Path = new Path(testDir, storeType + "_2.data");
    Appender appender2 = StorageManager.getAppender(conf, meta, table2Path);

    for(int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(4);
      vTuple.put(0, DatumFactory.createInt(i+1));
      vTuple.put(1, DatumFactory.createString("hyunsik"));
      vTuple.put(2, DatumFactory.createString("jihoon"));
      vTuple.put(3, DatumFactory.createLong(25l));
      appender2.addTuple(vTuple);
    }
    appender2.close();

    TableStat stat2 = appender2.getStats();
    if (stat2 != null) {
      assertEquals(tupleNum, stat2.getNumRows().longValue());
    }


    FileStatus status1 = fs.getFileStatus(table1Path);
    FileStatus status2 = fs.getFileStatus(table2Path);
    Fragment[] tablets = new Fragment[2];
    tablets[0] = new Fragment("tablet1", table1Path, meta, 0,
        status1.getLen(), null);
    tablets[1] = new Fragment("tablet1", table2Path, meta, 0,
        status2.getLen(), null);
    
    Scanner scanner = new MergeScanner(conf, meta, TUtil.newList(tablets));
    int totalCounts = 0;
    while (scanner.next() != null) {
      totalCounts++;
    }
    scanner.close();
    
    assertEquals(tupleNum * 2, totalCounts);
	}
}