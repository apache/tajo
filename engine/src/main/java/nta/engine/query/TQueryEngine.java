package nta.engine.query;

import java.io.IOException;

import nta.catalog.CatalogService;
import nta.engine.NConstants;
import nta.engine.SubqueryContext;
import nta.engine.exception.InternalException;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.json.GsonCreator;
import nta.engine.planner.PhysicalPlanner;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.physical.PhysicalExec;
import nta.storage.StorageManager;
import nta.zookeeper.ZkClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
    this.phyPlanner = new PhysicalPlanner(storageManager);
  }
  
  public PhysicalExec createPlan(SubqueryContext ctx, QueryUnitRequest request, 
      Path localTmpDir) throws InternalException {
    LogicalNode plan = GsonCreator.getInstance().
        fromJson(request.getSerializedData(), LogicalNode.class);
    LOG.info(plan.toString());
    LOG.info("Assigned task: (" + request.getId() + ") start:"
        + request.getFragments().get(0).getStartOffset() + " end: "
        + request.getFragments() + "\nplan:\n" + plan);

    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);
    
    return exec;
  }
  
  public void stop() throws IOException {
    this.defaultFS.close();
  }
}
