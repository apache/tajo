package tajo.storage;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.util.CommonTestingUtil;

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
		schema.addColumn("id",DataType.INT);
		schema.addColumn("age",DataType.INT);
		schema.addColumn("name",DataType.STRING);

		TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
		
		Tuple[] tuples = new Tuple[4];
		for(int i=0; i < tuples.length; i++) {
		  tuples[i] = new VTuple(3);
		  tuples[i].put(new Datum[] {
          DatumFactory.createInt(i),
		      DatumFactory.createInt(i+32),
		      DatumFactory.createString("name"+i)});
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

		int i=0;
		while(scanner.next() != null) {
			i++;
		}
		assertEquals(4,i);
	}
}
