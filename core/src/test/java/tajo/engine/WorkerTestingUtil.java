/**
 *
 */
package tajo.engine;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf;
import tajo.datum.DatumFactory;
import tajo.engine.ipc.protocolrecords.Fragment;
import tajo.engine.parser.ParseTree;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.planner.LogicalOptimizer;
import tajo.engine.planner.LogicalPlanner;
import tajo.engine.planner.PhysicalPlanner;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.planner.physical.PhysicalExec;
import tajo.engine.query.ResultSetImpl;
import tajo.engine.utils.TUtil;
import tajo.storage.*;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;
import java.util.UUID;

/**
 * @author Hyunsik Choi
 *
 */
public class WorkerTestingUtil {

	public static void buildTestDir(String dir) throws IOException {
		Path path = new Path(dir);
		FileSystem fs = FileSystem.getLocal(new Configuration());
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

	public static void writeTmpTable(TajoConf conf, String parent,
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

  private TajoConf conf;
  private CatalogService catalog;
  private SubqueryContext.Factory factory;
  private QueryAnalyzer analyzer;
  public WorkerTestingUtil(TajoConf conf) throws IOException {
    this.conf = conf;
    this.catalog = new LocalCatalog(conf);
    factory = new SubqueryContext.Factory();
    analyzer = new QueryAnalyzer(catalog);
  }

  public ResultSet run(String [] tableNames, File [] tables, Schema [] schemas, String query)
      throws IOException {
    File workDir = createTmpTestDir();
    StorageManager sm = StorageManager.get(new TajoConf(), workDir.getAbsolutePath());
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
