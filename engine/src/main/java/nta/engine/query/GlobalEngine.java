/**
 * 
 */
package nta.engine.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import nta.catalog.CatalogService;
import nta.catalog.Column;
import nta.catalog.HostInfo;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.EngineService;
import nta.engine.NtaEngineMaster;
import nta.engine.QueryContext;
import nta.engine.QueryUnitId;
import nta.engine.QueryUnitProtos.InProgressStatus;
import nta.engine.SubQueryId;
import nta.engine.cluster.QueryManager;
import nta.engine.cluster.WorkerCommunicator;
import nta.engine.exception.NTAQueryException;
import nta.engine.exec.eval.ConstEval;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.FieldEval;
import nta.engine.exec.eval.FuncCallEval;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.ipc.protocolrecords.QueryUnitRequest;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.global.GlobalQueryPlan;
import nta.engine.planner.global.QueryStep;
import nta.engine.planner.global.QueryStep.Phase;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalRootNode;
import nta.engine.planner.logical.ProjectionNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SelectionNode;
import nta.engine.planner.logical.CreateTableNode;
import nta.storage.StorageManager;
import nta.storage.Tuple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;

/**
 * @author jihoon
 * 
 */
public class GlobalEngine implements EngineService {
  private Log LOG = LogFactory.getLog(GlobalEngine.class);

  private final Configuration conf;
  private final CatalogService catalog;
  private final QueryAnalyzer analyzer;
  private final StorageManager storageManager;
  private final QueryContext.Factory factory;
  private final StorageManager sm;

  private GlobalQueryPlanner globalPlanner;
  private WorkerCommunicator wc;
  private QueryManager qm;
  
  private NtaEngineMaster master;
  
  public GlobalEngine(Configuration conf, CatalogService cat,
      StorageManager sm, WorkerCommunicator wc,
      QueryManager qm, NtaEngineMaster master)
      throws IOException {
    this.conf = conf;
    this.catalog = cat;
    this.storageManager = sm;
    this.wc = wc;
    this.qm = qm;
    this.master = master;
    this.analyzer = new QueryAnalyzer(cat);
    this.factory = new QueryContext.Factory(catalog);
    this.sm = new StorageManager(conf);

    this.globalPlanner = new GlobalQueryPlanner(this.catalog, this.sm);
  }

  public void createTable(TableDesc meta) throws IOException {
    catalog.addTable(meta);
  }

  private String buildFormatString(String query, String storeName) {
    String[] tokens = query.split(" ");
    String formatStr = storeName + " := ";
    for (int i = 0; i < tokens.length; i++) {
      if (tokens[i].equals("from")) {
        formatStr += "from %s ";
        i++;
      } else {
        formatStr += tokens[i] + " ";
      }
    }
    return formatStr;
  }

  public String executeQuery(SubQueryId subQueryId, String querystr) throws Exception {
    LOG.info("* issued query: " + querystr);
    // build the logical plan
    QueryContext ctx = factory.create();
    ParseTree tree = (QueryBlock) analyzer.parse(ctx, querystr);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, tree);
    LogicalOptimizer.optimize(ctx, plan);
    LOG.info("* logical plan:\n" + plan);

    // build the global plan
    GlobalQueryPlan globalPlan = globalPlanner.build(subQueryId, plan);

    long before = System.currentTimeMillis();
    
    // execute sub queries
    QueryStep queryStep = null, prevStep = null, tmpStep = null;
    QueryUnit q = null;
    HostInfo servInfo;
    int i, j;
    QueryUnitRequest request = null;
    QueryUnit[] units;
    
    for (i = 0; i < globalPlan.size(); i++) {
      tmpStep = globalPlan.getQueryStep(i);
      
      LOG.info("Table path " + tmpStep.getId() + " is initialized for " + tmpStep.getId());
      if (tmpStep.getPhase() == Phase.MAP) {
        sm.getFileSystem().mkdirs(sm.getTablePath(tmpStep.getId().toString()));
      } else {
        
        sm.initTableBase(TCatUtil.newTableMeta(tmpStep.getQuery(0).getOutputSchema(), StoreType.CSV), 
            tmpStep.getId().toString());
      }

      queryStep = globalPlanner.localize(tmpStep, master.getOnlineServer().size());
      
      // set host info
      selectRandomHostForQuery(queryStep);

      // send UnitQueryRequest
      for (j = 0; j < queryStep.size(); j++) {
        q = queryStep.getQuery(j);
        request = new QueryUnitRequestImpl(q.getId(), q.getFragments(), 
            q.getOutputName(), false, q.buildLogicalPlan().toJSON());
        wc.requestQueryUnit(q.getHost(), q.getPort(), request.getProto());
        LOG.info("QueryUnitRequest " + q.getId() + " is sent to " + (q.getHost()+":"+q.getPort()));
        LOG.info("QueryStep's output name " + q.getStoreTableNode().getTableName());
      }

      prevStep = queryStep;

      // wait for finishing the query step
      if (prevStep != null) {
        waitForFinishUnitQueries(prevStep);
      }
    }

    long after = System.currentTimeMillis();
    LOG.info("executeQuery processing time: " + (after - before) + "msc");

    return "";
  }
  
  private String initOutputTable(LogicalNode plan) throws IOException {
    String storeName = null;
    if (((LogicalRootNode) plan).getSubNode().getType() == ExprType.STORE) {
      CreateTableNode storeNode = (CreateTableNode) ((LogicalRootNode) plan)
          .getSubNode();
      storeName = storeNode.getTableName();
    } else {
      storeName = "t_" + System.currentTimeMillis();
    }
//    String formatStr = buildFormatString(querystr, storeName);
    Schema outSchema = ((LogicalRootNode) plan).getSubNode().getOutputSchema();
    TableMeta meta = TCatUtil.newTableMeta(outSchema, StoreType.CSV);
    sm.initTableBase(meta, storeName);
    LOG.info(">>>>> Output directory (" + sm.getTablePath(storeName)
        + ") is created");
    return storeName;
  }

  private void waitForFinishUnitQueries(QueryStep step) throws InterruptedException {
    int i;
    Float progress = null;
    boolean wait = true;
    while (wait) {
      Thread.sleep(1000);
      LOG.info("><><><><><><>< InProgressQueries: " + qm.getAllProgresses());
      for (i = 0; i < step.size(); i++) {
        // progress = qm.getProgress(step.getQuery(i).getId());
        InProgressStatus inprogress = qm.getProgress(step.getQuery(i).getId());
        if (inprogress != null) {
          progress = inprogress.getProgress();
          if (progress != null && progress < 1.f) {
            break;
          }
        } else {
          break;
        }
      }
      if (i == step.size()) {
        wait = false;
      }
    }
  }
  
  private void setFragment(QueryUnit q) throws IOException {
    Fragment[] frags = sm.split(sm.getTablePath(q.getInputName()));
    for (Fragment frag : frags) {
      frag.setId(q.getInputName());
    }
    q.addFragments(frags);
    
  }
  
//  private HostInfo selectProperHostForQuery(QueryUnit q) throws IOException, 
//  KeeperException, InterruptedException {
//    List<HostInfo> fragmentServInfo = null;
//
////    if (q.getFragments().size() != 0) {
////      fragmentServInfo = catalog.getHostByTable(q.getTableName());
////    } else {
////      Set<QueryUnit> prevs = q.getPrevQueries();
////      if (!prevs.isEmpty()) {
////        Iterator<QueryUnit> it = prevs.iterator();
////        while (it.hasNext()) {
////          // fill table name. TODO: 하나 이상의 입력 테이블 지원
////          // fill fragments
////          QueryUnit next = it.next();
////          Fragment[] frags = sm.split(sm.getTablePath(next.getOutputName()));
////          q.addFragments(frags);
////        }
////      } else {
////        // TODO: can not find input sources
////      }
////      fragmentServInfo = this.getFragmentLocInfo(sm.getTablePath(q.getTableName()), 
////          q.getOp().getInputSchema(), StoreType.CSV);
////    }
//
//    fragmentServInfo = catalog.getHostByTable(q.getInputName());
//    if (fragmentServInfo == null) {
//      fragmentServInfo = this.getFragmentLocInfo(q.getInputName(), sm.getTablePath(q.getInputName()), 
//          q.getOp().getInputSchema(), StoreType.CSV);
//    }
//
//    for (HostInfo servInfo : fragmentServInfo) {
//      if (servInfo.getFragment().equals(q.getFragments().get(0))) {
//        return servInfo;
//      }
//    }
//    // can not find the fragment serving information
//    // TODO: fill host and port
//    return selectRandomHostForQuery(q);
//  }
  
  private void selectRandomHostForQuery(QueryStep step) 
      throws KeeperException, InterruptedException {
    List<String> serverNames = master.getOnlineServer();
    Random rand = new Random();
    QueryUnit unit;
    for (int i = 0; i < step.size(); i++) {
      unit = step.getQuery(i);
      StringTokenizer tokenizer = new StringTokenizer(serverNames.get(
          rand.nextInt(serverNames.size())), ":");
      unit.setHost(tokenizer.nextToken(":"), Integer.valueOf(tokenizer.nextToken(":")));
    }
  }
  
//  private QueryStep selectHostforPartitions(QueryStep step, String tableName) 
//      throws IOException, KeeperException, InterruptedException {
//    QueryStep newStep = new QueryStep(step.getSubQueryId());
//    SortedMap<String, List<Path>> hashed = new TreeMap<String, List<Path>>();    
//    Path partitionPath = sm.getTablePath(step.getQuery(0).getInputName());
//    LOG.info("Get partition dir (" + partitionPath.toString() + ")");
//    FileStatus [] outputs = sm.getFileSystem().listStatus(partitionPath);
//    TableMeta meta = sm.getTableMeta(outputs[0].getPath());
//    
//    for (FileStatus s : outputs) {
//      for (FileStatus d : sm.getTableDataFiles(s.getPath())) {
//        if (hashed.containsKey(d.getPath().getName())) {
//          hashed.get(d.getPath().getName()).add(d.getPath());
//        } else {
//          List<Path> l = new ArrayList<Path>();
//          l.add(d.getPath());
//          hashed.put(d.getPath().getName(), l);
//        }
//      }
//    }
//    
//    Iterator<Entry<String,List<Path>>> it = hashed.entrySet().iterator();
//    int assignNum = (int) Math.ceil((float)hashed.size() / (float)step.size());
//    QueryUnit unit = null;
//    QueryUnit[] units;
//    List<Fragment> fragments = new ArrayList<Fragment>();
//    
//    List<String> hosts = master.getOnlineServer();
//    int k = 0;
//    for (int i = 0; i < step.size(); i++) {
//      unit = step.getQuery(i);
//      String outputName = unit.getPrevQueries().iterator().next()
//          .getOutputName();
//      
//      // fill the fragments
//      fragments = new ArrayList<Fragment>();
//      // assign a fragment set to each query unit
//      for (int j = 0; j < assignNum && it.hasNext(); j++) {        
//        Entry<String,List<Path>> entry = it.next();       
//        
//        for (Path path : entry.getValue()) {
//          Fragment frag = sm.getFragment(outputName, meta, path);
//          fragments.add(frag);
//        }
//      }
//      unit.setFragments(fragments.toArray(new Fragment[fragments.size()]));
//      
//      units = globalPlanner.splitQueryUnitByHash(unit, hashed.size());
//      
//      for (QueryUnit u : units) {
//        String [] split = hosts.get(k).split(":");
//        u.setHost(split[0], Integer.valueOf(split[1]));
//        k++;
//        if (k > hosts.size() - 1) {
//          k = 0;
//        }
//        newStep.addQuery(u);
//      }
//    }
//    return newStep;
//  }
  
  public Map<QueryUnitId, Float> getProgress(SubQueryId subqid) {
    Map<QueryUnitId, Float> progressMap = new HashMap<QueryUnitId, Float>();
    
    
    
    return progressMap;
  }
  
  private String tupleToString(Tuple t) {
    boolean first = true;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < t.size(); i++) {
      if (t.get(i) != null) {
        if (first) {
          first = false;
        } else {
          sb.append("\t");
        }
        sb.append(t.get(i));
      }
    }
    return sb.toString();
  }

  private List<HostInfo> getFragmentLocInfo(String tableId, Path tbPath, Schema schema,
      StoreType type) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus file = fs.getFileStatus(tbPath);
    String[] hosts;
    int i = 0;
    BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, file.getLen());
    List<HostInfo> result = new ArrayList<HostInfo>();
    for (int j = 0; j < blocks.length; j++) {
      hosts = blocks[j].getHosts();
      result.add(new HostInfo(hosts[0], -1, new Fragment(tableId, 
          tbPath, TCatUtil.newTableMeta(schema, type), 0, file.getLen())));
      ++i;
    }
    return result;
  }

  public void updateQuery(String nql) throws NTAQueryException {

  }

  /*
   * (non-Javadoc)
   * 
   * @see nta.engine.EngineService#init()
   */
  @Override
  public void init() throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see nta.engine.EngineService#shutdown()
   */
  @Override
  public void shutdown() throws IOException {
    LOG.info(GlobalEngine.class.getName() + " is being stopped");
  }

  private String generateQuery(LogicalNode plan, Fragment fragment,
      String groupby) {
    LogicalNode op = plan;
    String strQuery = "";
    String from = "";
    String where = "";
    String proj = "*";
    ArrayList<LogicalNode> q = new ArrayList<LogicalNode>();
    q.add(op);

    while (!q.isEmpty()) {
      op = q.remove(0);
      switch (op.getType()) {
      case SCAN:
        ScanNode scan = (ScanNode) op;
        from = fragment.getId();
        if (scan.hasTargetList()) {
          proj = "";
          for (Column te : scan.getTargetList().getColumns()) {
            if (proj.equals("")) {
              proj += te.getColumnName();
            } else {
              proj += ", " + te.getColumnName();
            }
          }
        }
        if (scan.hasQual()) {
          where = exprToString(scan.getQual());
        }
        break;
      case SELECTION:
        SelectionNode sel = (SelectionNode) op;
        where = exprToString(sel.getQual());
        q.add(sel.getSubNode());
        break;
      case PROJECTION:
        ProjectionNode projop = (ProjectionNode) op;
        proj = "";
        for (Target te : projop.getTargetList()) {
          proj += te.getColumnSchema().getColumnName() + ", ";
        }
        q.add(projop.getSubNode());
        break;
      default:
        break;
      }
    }

    strQuery = "select " + proj;
    if (!from.equals("")) {
      strQuery += " from " + from;
    }
    if (!where.equals("")) {
      strQuery += " where " + where;
    }
    if (groupby != null) {
      strQuery += groupby;
    }
    return strQuery;
  }

  private String exprToString(EvalNode expr) {
    String str = "";
    ArrayList<EvalNode> s = new ArrayList<EvalNode>();
    EvalNode e;
    s.add(expr);
    s.add(expr);

    while (!s.isEmpty()) {
      e = s.remove(0);
      if (s.size() > 0 && e.equals(s.get(0))) {
        // in
        if (e.getRightExpr() != null) {
          s.add(1, e.getRightExpr());
          s.add(1, e.getRightExpr());
        }
        if (e.getLeftExpr() != null) {
          s.add(0, e.getLeftExpr()); // in
          s.add(0, e.getLeftExpr()); // out
        }
      } else {
        // out
        str += getStringOfExpr(e) + " ";
      }
    }

    return str;
  }

  private String getStringOfExpr(EvalNode expr) {
    String ret = null;
    switch (expr.getType()) {
    case FIELD:
      FieldEval field = (FieldEval) expr;
      ret = field.getColumnName();
      break;
    case FUNCTION:
      FuncCallEval func = (FuncCallEval) expr;
      ret = func.getName();
      break;
    case AND:
      ret = "AND";
      break;
    case OR:
      ret = "OR";
      break;
    case CONST:
      ConstEval con = (ConstEval) expr;
      ret = con.toString();
      break;
    case PLUS:
      ret = "+";
      break;
    case MINUS:
      ret = "-";
      break;
    case MULTIPLY:
      ret = "*";
      break;
    case DIVIDE:
      ret = "/";
      break;
    case EQUAL:
      ret = "=";
      break;
    case NOT_EQUAL:
      ret = "!=";
      break;
    case LTH:
      ret = "<";
      break;
    case LEQ:
      ret = "<=";
      break;
    case GTH:
      ret = ">";
      break;
    case GEQ:
      ret = ">=";
      break;
    }
    return ret;
  }
}
