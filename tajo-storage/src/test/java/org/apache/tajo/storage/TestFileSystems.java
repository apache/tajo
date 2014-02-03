package org.apache.tajo.storage;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.s3.InMemoryFileSystemStore;
import org.apache.tajo.storage.s3.SmallBlockS3FileSystem;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestFileSystems {

  protected byte[] data = null;

  private static String TEST_PATH = "target/test-data/TestFileSystem";
  private TajoConf conf = null;
  private AbstractStorageManager sm = null;
  private FileSystem fs = null;
  Path testDir;

  public TestFileSystems(FileSystem fs) throws IOException {
    conf = new TajoConf();
    sm = StorageManagerFactory.getStorageManager(conf);

    this.fs = fs;
    fs.initialize(URI.create(fs.getScheme() + ":///"), conf);
    testDir = getTestDir(this.fs, TEST_PATH);
  }

  public Path getTestDir(FileSystem fs, String dir) throws IOException {
    Path path = new Path(dir);
    if(fs.exists(path))
      fs.delete(path, true);

    fs.mkdirs(path);

    return fs.makeQualified(path);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Parameterized.Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][] {
        {new SmallBlockS3FileSystem(new InMemoryFileSystemStore())},
    });
  }

  @Test
  public void testBlockSplit() throws IOException {

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("age", Type.INT4);
    schema.addColumn("name", Type.TEXT);

    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV);

    Tuple[] tuples = new Tuple[4];
    for (int i = 0; i < tuples.length; i++) {
      tuples[i] = new VTuple(3);
      tuples[i]
          .put(new Datum[] { DatumFactory.createInt4(i),
              DatumFactory.createInt4(i + 32),
              DatumFactory.createText("name" + i) });
    }

    Path path = StorageUtil.concatPath(testDir, "testGetScannerAndAppender",
        "table.csv");
    fs.mkdirs(path.getParent());

    Appender appender = sm.getAppender(meta, schema, path);
    appender.init();
    for (Tuple t : tuples) {
      appender.addTuple(t);
    }
    appender.close();
    FileStatus fileStatus = fs.getFileStatus(path);

    List<FileFragment> splits = sm.getSplits("table", meta, schema, path);
    int splitSize = (int) Math.ceil(fileStatus.getLen() / (double) fileStatus.getBlockSize());
    assertEquals(splits.size(), splitSize);

  }


}
