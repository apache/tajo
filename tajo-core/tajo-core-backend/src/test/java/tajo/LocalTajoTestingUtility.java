package tajo;

import com.google.protobuf.ServiceException;
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
  private TajoTestingCluster util;
  private TajoConf conf;
  private TajoClient client;

  public void setup(String[] names,
                    String[] tablepaths,
                    Schema[] schemas,
                    Options option) throws Exception {

    util = new TajoTestingCluster();
    util.startMiniCluster(1);
    conf = util.getConfiguration();
    client = new TajoClient(conf);

    FileSystem fs = util.getDefaultFileSystem();
    Path rootDir = util.getMaster().
        getStorageManager().getBaseDir();
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
      client.createTable(names[i], tablePath, meta);
    }
  }

  public TajoTestingCluster getTestingCluster() {
    return util;
  }

  public ResultSet execute(String query) throws IOException, ServiceException {
    return client.executeQueryAndGetResult(query);
  }

  public void shutdown() throws IOException {
    client.close();
    util.shutdownMiniCluster();
  }
}
