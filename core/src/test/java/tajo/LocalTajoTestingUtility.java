package tajo;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.catalog.Options;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos;
import tajo.client.TajoClient;
import tajo.conf.TajoConf;

import java.io.IOException;
import java.sql.ResultSet;

/**
 * @author Hyunsik Choi
 */
public class LocalTajoTestingUtility {
  private tajo.engine.TajoTestingUtility util;
  private TajoConf conf;
  private TajoClient client;

  public void setup(String[] names,
                    String[] tablepaths,
                    Schema[] schemas,
                    Options option) throws Exception {

    util = new tajo.engine.TajoTestingUtility();
    util.startMiniCluster(1);
    conf = util.getConfiguration();

    FileSystem fs = util.getDefaultFileSystem();
    Path rootDir = util.getMiniTajoCluster().getMaster().
        getStorageManager().getDataRoot();
    fs.mkdirs(rootDir);
    for (int i = 0; i < tablepaths.length; i++) {
      Path localPath = new Path(tablepaths[i]);
      Path tablePath = new Path(rootDir, names[i]);
      fs.mkdirs(tablePath);
      Path dataPath = new Path(tablePath, "data");
      fs.mkdirs(dataPath);
      Path dfsPath = new Path(dataPath, localPath.getName());
      fs.copyFromLocalFile(localPath, dfsPath);
      TableMeta meta = TCatUtil.newTableMeta(schemas[i],
          CatalogProtos.StoreType.CSV, option);
      client = new TajoClient(conf);
      client.createTable(names[i], tablePath, meta);
    }
  }

  public ResultSet execute(String query) throws IOException {
    return client.executeQuery(query);
  }

  public void shutdown() throws IOException {
    util.shutdownMiniCluster();
  }
}
