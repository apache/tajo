package org.apache.tajo.experiment;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.client.ResultSetUtil;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.planner.physical.PhysicalExec;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.columnar.VecRowBlock;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.parquet.ParquetScanner;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.Test;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.VecRowParquetReader;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TpchQ1 {

  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static CatalogService catalog;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static AbstractStorageManager sm;
  private static Path testDir;
  private static Session session = LocalTajoTestingUtility.createDummySession();

  private static MasterPlan masterPlan;

  public TpchQ1() throws Exception {
    util = new TajoTestingCluster();

    util.startCatalogCluster();
    conf = util.getConfiguration();
    testDir = CommonTestingUtil.getTestDir("target/test-data/TestPhysicalPlanner");
    sm = StorageManagerFactory.getStorageManager(conf, testDir);
    catalog = util.getMiniCatalogCluster().getCatalog();
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.createFunction(funcDesc);
    }

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(conf);

    masterPlan = new MasterPlan(LocalTajoTestingUtility.newQueryId(), null, null);
  }

  public void shutdown() {
    util.shutdownCatalogCluster();
  }

  static final Schema LINEITEM;

  static {
    LINEITEM = new Schema()
        .addColumn("l_orderkey", TajoDataTypes.Type.INT4) // 0
        .addColumn("l_partkey", TajoDataTypes.Type.INT4) // 1
        .addColumn("l_suppkey", TajoDataTypes.Type.INT4) // 2
        .addColumn("l_linenumber", TajoDataTypes.Type.INT4) // 3
        .addColumn("l_quantity", TajoDataTypes.Type.FLOAT8) // 4
        .addColumn("l_extendedprice", TajoDataTypes.Type.FLOAT8) // 5
        .addColumn("l_discount", TajoDataTypes.Type.FLOAT8) // 6
        .addColumn("l_tax", TajoDataTypes.Type.FLOAT8) // 7
        .addColumn("l_returnflag", TajoDataTypes.Type.CHAR, 1) // 8
        .addColumn("l_linestatus", TajoDataTypes.Type.CHAR, 1) // 9
        .addColumn("l_shipdate", TajoDataTypes.Type.CHAR, 10) // 10
        .addColumn("l_commitdate", TajoDataTypes.Type.CHAR, 10) // 11
        .addColumn("l_receiptdate", TajoDataTypes.Type.CHAR, 10) // 12
        .addColumn("l_shipinstruct", TajoDataTypes.Type.CHAR, 10) // 13
        .addColumn("l_shipmode", TajoDataTypes.Type.TEXT) // 14
        .addColumn("l_comment", TajoDataTypes.Type.TEXT); // 15
  }

  @Test
  public void generateTuples() throws IOException, SQLException, ServiceException, PlanningException {
    Path rawLineitemPath = new Path("file:///home/hyunsik/Code/tpch_2_15_0/dbgen/lineitem.tbl");

    KeyValueSet kv = new KeyValueSet();
    kv.put(StorageConstants.CSVFILE_DELIMITER, "\\|");
    TableMeta meta = new TableMeta(CatalogProtos.StoreType.CSV, kv);

    TableDesc tabledesc = new TableDesc("default.lineitem", LINEITEM, meta, rawLineitemPath, true);
    catalog.createTable(tabledesc);


    FileSystem localFS = FileSystem.getLocal(conf);

    long length = localFS.getLength(rawLineitemPath);

    FileFragment[] frags = new FileFragment[1];
    frags[0] = new FileFragment("default.lineitem", rawLineitemPath, 0, length, null);

    Path workDir = CommonTestingUtil.getTestDir("file:///home/hyunsik/experiment/test-data");

    TaskAttemptContext ctx = new TaskAttemptContext(conf, LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setOutputPath(new Path("file:///home/hyunsik/experiment/data/lineitem.parquet"));
    ctx.setEnforcer(new Enforcer());
    Expr context = analyzer.parse("insert overwrite into location 'file:///home/hyunsik/lineitem.parquet' USING PARQUET WITH ('parquet.block.size' = '1073741824') SELECT * FROM default.lineitem");

    LogicalPlan plan = planner.createPlan(session, context);
    optimizer.optimize(plan);
    LogicalNode rootNode = plan.getRootBlock().getRoot();
    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    while(exec.next() != null) {
    }
    exec.close();
  }

  @Test
  public void processQ1InTupleWay() throws IOException, SQLException, ServiceException, PlanningException {
    Path rawLineitemPath = new Path("file:///home/hyunsik/experiment/data/lineitem.parquet");

    KeyValueSet kv = StorageUtil.newPhysicalProperties(CatalogProtos.StoreType.PARQUET);
    TableMeta meta = new TableMeta(CatalogProtos.StoreType.PARQUET, kv);

    TableDesc tabledesc = new TableDesc("default.lineitem", LINEITEM, meta, rawLineitemPath, true);
    catalog.createTable(tabledesc);

    FileSystem localFS = FileSystem.getLocal(conf);
    long length = localFS.getLength(rawLineitemPath);

    FileFragment[] frags = new FileFragment[1];
    frags[0] = new FileFragment("default.lineitem", rawLineitemPath, 0, length, null);
    Path workDir = CommonTestingUtil.getTestDir("file:///home/hyunsik/experiment/test-data");

    Expr expr = analyzer.parse(FileUtil.readTextFileFromResource("experiment/q1.sql"));
    LogicalPlan plan = planner.createPlan(session, expr);
    optimizer.optimize(plan);

    Enforcer enforcer = new Enforcer();
    GroupbyNode groupbyNode = PlannerUtil.findTopNode(plan.getRootBlock().getRoot(), NodeType.GROUP_BY);
    for (AggregationFunctionCallEval eval : groupbyNode.getAggFunctions()) {
      eval.setFirstPhase();
    }
    enforcer.enforceHashAggregation(groupbyNode.getPID());

    TaskAttemptContext ctx = new TaskAttemptContext(conf, LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setOutputPath(new Path("file:///home/hyunsik/experiment/test-data"));
    ctx.setEnforcer(enforcer);

    System.out.println(plan);

    LogicalNode rootNode = plan.getRootBlock().getRoot();
    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();

    long beginProcess = System.currentTimeMillis();
    int resultCount = 0;
    while(exec.next() != null) {
      resultCount++;
    }
    long endProcess = System.currentTimeMillis();
    exec.close();
    System.out.println((endProcess - beginProcess) + " msec (" + resultCount + " rows)");
  }

  @Test
  public void testQ1InVectorization() throws IOException {
    KeyValueSet KeyValueSet = StorageUtil.newPhysicalProperties(CatalogProtos.StoreType.PARQUET);
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.PARQUET, KeyValueSet);
    meta.putOption(ParquetOutputFormat.COMPRESSION, CompressionCodecName.UNCOMPRESSED.name());

    Set<String> projectedNames = Sets.newHashSet(
        "l_returnflag",
        "l_linestatus",
        "l_quantity",
        "l_extendedprice",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_shipdate"
    );



    Schema projected = new Schema();
    for (Column p : LINEITEM.toArray()) {
      if (projectedNames.contains(p.getQualifiedName())) {
        projected.addColumn(p);
      }
    }

    System.out.println(">>>>>>" + projected);

    Path path = new Path("file:///home/hyunsik/experiment/data/lineitem.parquet");
    VecRowBlock vecRowBlock = new VecRowBlock(projected, 1024);
    VecRowParquetReader reader = new VecRowParquetReader(path, LINEITEM, projected);
    long readStart = System.currentTimeMillis();

    int rowIdx = 0;
    while(reader.nextFetch(vecRowBlock)) {

      for (int vectorId = 0; vectorId < vecRowBlock.maxVecSize(); vectorId++) {
//        System.out.println(vecRowBlock.getFloat8(0, vectorId));
//        System.out.println(vecRowBlock.getFloat8(1, vectorId));
//        System.out.println(vecRowBlock.getFloat8(2, vectorId));
//        System.out.println(vecRowBlock.getFloat8(3, vectorId));
//        System.out.println(new String(vecRowBlock.getFixedText(4, vectorId)));
//        System.out.println(new String(vecRowBlock.getFixedText(5, vectorId)));
//        System.out.println(new String(vecRowBlock.getFixedText(6, vectorId)));

        //assertEquals(rowId % 2, vecRowBlock.getBool(0, vectorId));
        //assertTrue(1 == vecRowBlock.getInt2(1, vectorId));
        //assertEquals(rowId, vecRowBlock.getInt4(2, vectorId));
//        assertEquals(rowId, vecRowBlock.getInt8(3, vectorId));
        //assertTrue(rowId == vecRowBlock.getFloat4(4, vectorId));
        //assertTrue(((double)rowId) == vecRowBlock.getFloat8(5, vectorId));
        //assertEquals("colabcdefghijklmnopqrstu1", (vecRowBlock.getString(6, vectorId)));
        //assertEquals("colabcdefghijklmnopqrstu2", (vecRowBlock.getString(7, vectorId)));
        rowIdx++;
      }
      vecRowBlock.clear();
    }
    long readEnd = System.currentTimeMillis();
    System.out.println(rowIdx + " rows (" + (readEnd - readStart) + " read msec)");
    vecRowBlock.free();
  }

  public static FileFragment createFileFragment(String name, Path path) throws IOException {
    FileSystem localFS = FileSystem.getLocal(conf);
    long length = localFS.getLength(path);
    FileFragment ff = new FileFragment(name, path, 0, length);
    return ff;
  }

  @Test
  public void testReadParquet() throws IOException {
    Configuration conf = new Configuration();

    KeyValueSet KeyValueSet = StorageUtil.newPhysicalProperties(CatalogProtos.StoreType.PARQUET);
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.PARQUET, KeyValueSet);
    meta.putOption(ParquetOutputFormat.COMPRESSION, CompressionCodecName.UNCOMPRESSED.name());

    Schema projected = new Schema();
    projected.addColumn("l_orderkey", TajoDataTypes.Type.INT4, 0);
    projected.addColumn("l_partkey", TajoDataTypes.Type.INT4, 1);


    Path path = new Path("file:///home/hyunsik/experiment/data/lineitem.parquet");
    VecRowBlock vecRowBlock = new VecRowBlock(LINEITEM, 1024);
    VecRowParquetReader reader = new VecRowParquetReader(path, LINEITEM, projected);
    long readStart = System.currentTimeMillis();

    int rowIdx = 0;
    while(reader.nextFetch(vecRowBlock)) {
      for (int vectorId = 0; vectorId < vecRowBlock.maxVecSize(); vectorId++) {
        assertTrue(vecRowBlock.getInt4(0, vectorId) > 0);
        assertTrue(vecRowBlock.getInt4(1, vectorId) > 0);
        //assertEquals(rowId % 2, vecRowBlock.getBool(0, vectorId));
        //assertTrue(1 == vecRowBlock.getInt2(1, vectorId));
        //assertEquals(rowId, vecRowBlock.getInt4(2, vectorId));
//        assertEquals(rowId, vecRowBlock.getInt8(3, vectorId));
        //assertTrue(rowId == vecRowBlock.getFloat4(4, vectorId));
        //assertTrue(((double)rowId) == vecRowBlock.getFloat8(5, vectorId));
        //assertEquals("colabcdefghijklmnopqrstu1", (vecRowBlock.getString(6, vectorId)));
        //assertEquals("colabcdefghijklmnopqrstu2", (vecRowBlock.getString(7, vectorId)));
        rowIdx++;
      }
      vecRowBlock.clear();
    }
    long readEnd = System.currentTimeMillis();
    System.out.println(rowIdx + " rows (" + (readEnd - readStart) + " read msec)");
    vecRowBlock.free();
  }
}
