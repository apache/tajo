package nta.engine.query;

import java.io.IOException;

import nta.catalog.CatalogService;
import nta.engine.NConstants;
import nta.engine.SubqueryContext;
import nta.engine.exception.InternalException;
import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
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
  
  private final Configuration conf;  
  private final FileSystem defaultFS;
  
  private final CatalogService catalog;
  private final StorageManager storageManager;
  
  private final Path basePath;
  private final Path dataPath;
  
  private final QueryAnalyzer analyzer;
  private final SubqueryContext.Factory ctxFactory;
  
  public TQueryEngine(Configuration conf, CatalogService catalog, 
      ZkClient zkClient) throws IOException {
    this.conf = conf;
    this.catalog = catalog;
    
    // Get the tajo base dir
    this.basePath = new Path(conf.get(NConstants.ENGINE_BASE_DIR));
    LOG.info("Base dir is set " + conf.get(NConstants.ENGINE_BASE_DIR));
    
    // Get default DFS uri from the base dir
    this.defaultFS = basePath.getFileSystem(conf);
    LOG.info("FileSystem (" + this.defaultFS.getUri() + ") is initialized.");

    this.dataPath = new Path(conf.get(NConstants.ENGINE_DATA_DIR));
    LOG.info("Tajo data dir is set " + dataPath);
        
    this.storageManager = new StorageManager(conf);
    this.analyzer = new QueryAnalyzer(catalog);
    this.ctxFactory = new SubqueryContext.Factory(catalog);
  }
  
  public PhysicalExec createPlan(SubQueryRequest request) throws InternalException {
    SubqueryContext ctx = ctxFactory.create(request);
    QueryBlock query = analyzer.parse(ctx, request.getQuery());
    LogicalNode plan = LogicalPlanner.createPlan(ctx, query);
    LogicalOptimizer.optimize(ctx, plan);
    LOG.info("Assigned task: (" + request.getId() + ") start:"
        + request.getFragments().get(0).getStartOffset() + " end: "
        + request.getFragments() + "\nquery: " + request.getQuery());

    PhysicalPlanner phyPlanner = new PhysicalPlanner(storageManager);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);
    
    return exec;
  }
  
  public void stop() throws IOException {
    this.defaultFS.close();
  }
}
