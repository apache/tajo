/**
 *
 */
package nta.engine;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import nta.catalog.*;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.datum.DatumFactory;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.ParseTree;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.PhysicalPlanner;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.physical.PhysicalExec;
import nta.engine.query.ResultSetImpl;
import nta.engine.utils.TUtil;
import nta.storage.Appender;
import nta.storage.StorageManager;
import nta.storage.StorageUtil;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Hyunsik Choi
 *
 */
public class WorkerTestingUtil {

	public static void buildTestDir(String dir) throws IOException {
		Path path = new Path(dir);
		FileSystem fs = path.getFileSystem(new Configuration());
		if(fs.exists(path))
			fs.delete(path, true);

		fs.mkdirs(path);
	}

	public final static Schema mockupSchema;
	public final static TableMeta mockupMeta;

	static {
    mockupSchema = new Schema();
    mockupSchema.addColumn("deptname", DataType.STRING);
    mockupSchema.addColumn("score", DataType.INT);
    mockupMeta = TCatUtil.newTableMeta(mockupSchema, StoreType.CSV);
	}

	public static void writeTmpTable(Configuration conf, String parent,
	    String tbName, boolean writeMeta) throws IOException {
	  StorageManager sm = StorageManager.get(conf, parent);

    Appender appender;
    if (writeMeta) {
      appender = sm.getTableAppender(mockupMeta, tbName);
    } else {
      FileSystem fs = sm.getFileSystem();
      fs.mkdirs(StorageUtil.concatPath(parent, tbName, "data"));
      appender = sm.getAppender(mockupMeta,
          StorageUtil.concatPath(parent, tbName, "data", "tb000"));
    }
    int deptSize = 10000;
    int tupleNum = 100;
    Tuple tuple;
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(2);
      String key = "test" + (i % deptSize);
      tuple.put(0, DatumFactory.createString(key));
      tuple.put(1, DatumFactory.createInt(i + 1));
      appender.addTuple(tuple);
    }
    appender.close();
	}

  private Configuration conf;
  private CatalogService catalog;
  private SubqueryContext.Factory factory;
  private QueryAnalyzer analyzer;
  public WorkerTestingUtil(Configuration conf) throws IOException {
    this.conf = conf;
    this.catalog = new LocalCatalog(conf);
    factory = new SubqueryContext.Factory();
    analyzer = new QueryAnalyzer(catalog);
  }

  public ResultSet run(String [] tableNames, File [] tables, Schema [] schemas, String query)
      throws IOException {
    File workDir = createTmpTestDir();
    StorageManager sm = StorageManager.get(NtaConf.create(), workDir.getAbsolutePath());
    List<Fragment> frags = Lists.newArrayList();
    for (int i = 0; i < tableNames.length; i++) {
      Fragment [] splits = sm.split(tableNames[i], new Path(tables[i].getAbsolutePath()));
      for (Fragment f : splits) {
        frags.add(f);
      }
    }

    SubqueryContext ctx = factory.create(TUtil.newQueryUnitAttemptId(),
        frags.toArray(new Fragment[frags.size()]), workDir);
    ParseTree tree = analyzer.parse(ctx, query);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, tree);
    plan = LogicalOptimizer.optimize(ctx, plan);
    PhysicalPlanner phyPlanner = new PhysicalPlanner(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    ResultSet result = new ResultSetImpl(conf, new File(workDir, "out").getAbsolutePath());
    return result;
  }

  public static File createTmpTestDir() {
    String randomStr = UUID.randomUUID().toString();
    File dir = new File("target/test-data", randomStr);
    // Have it cleaned up on exit
    dir.deleteOnExit();
    return dir;
  }
}
