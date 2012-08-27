package tajo.engine.query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.catalog.CatalogService;
import tajo.engine.NConstants;
import tajo.engine.SubqueryContext;
import tajo.engine.exception.InternalException;
import tajo.engine.planner.PhysicalPlanner;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.planner.physical.PhysicalExec;
import tajo.storage.StorageManager;
import tajo.zookeeper.ZkClient;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 */
public class TQueryEngine {
  private final static Log LOG = LogFactory.getLog(TQueryEngine.class);
  
  private final FileSystem defaultFS;
  private final StorageManager storageManager;
  
  private final Path basePath;
  private final Path dataPath;
  
  private final PhysicalPlanner phyPlanner;
  
  public TQueryEngine(Configuration conf, CatalogService catalog, 
      ZkClient zkClient) throws IOException {    
    // Get the tajo base dir
    this.basePath = new Path(conf.get(NConstants.ENGINE_BASE_DIR));
    LOG.info("Base dir is set " + conf.get(NConstants.ENGINE_BASE_DIR));
    
    // Get default DFS uri from the base dir
    this.defaultFS = basePath.getFileSystem(conf);
    LOG.info("FileSystem (" + this.defaultFS.getUri() + ") is initialized.");

    this.dataPath = new Path(conf.get(NConstants.ENGINE_DATA_DIR));
    LOG.info("Tajo data dir is set " + dataPath);
        
    this.storageManager = new StorageManager(conf);
    this.phyPlanner = new PhysicalPlanner(conf, storageManager);
  }
  
  public PhysicalExec createPlan(SubqueryContext ctx, LogicalNode plan) 
      throws InternalException {    
    return phyPlanner.createPlan(ctx, plan);
  }
  
  public void stop() throws IOException {
    this.defaultFS.close();
  }
}
