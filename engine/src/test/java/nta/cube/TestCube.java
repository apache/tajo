package nta.cube;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutionException;

import nta.catalog.CatalogService;
import nta.catalog.FunctionDesc;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.NtaTestingUtility;
import nta.engine.QueryContext;
import nta.engine.function.Aggavg;
import nta.engine.function.Aggcount;
import nta.engine.function.Aggmax;
import nta.engine.function.Aggmin;
import nta.engine.function.Aggsum;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.LogicalOptimizer;
import nta.engine.planner.LogicalPlanner;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalRootNode;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;

public class TestCube {

  NtaTestingUtility util;
  GroupbyNode gnode;
  String inputpath;
  String outputpath;
  public static String[] QUERIES = { "select src_net, aggsum(dst_net + dst_net2) from nta group by src_net" };

  @Before
  public void setUp() throws Exception {
    util = new NtaTestingUtility();
    util.startMiniZKCluster();
    util.startCatalogCluster();
    CatalogService catalog;
    catalog = util.getMiniCatalogCluster().getCatalog();
    /* test input generate */

    TestCubeSchema.datapath = new String("target/test-data/TestCube");
    Cons.datapath = TestCubeSchema.datapath;

    TestCubeSchema.SetOriginSchema();
    TestCubeSchema.datagen();
    Cons.immediatepath = new String("immediate");

    QueryContext.Factory factory;

    TableMeta meta = TCatUtil.newTableMeta(TestCubeSchema.TEST_SCHEMA,
        StoreType.CSV);
    TableDesc people = new TableDescImpl("nta", meta, new Path("file:///"));
    catalog.addTable(people);

    factory = new QueryContext.Factory(catalog);

    FunctionDesc funcMeta = new FunctionDesc("aggmin", Aggmin.class,
        FunctionType.AGGREGATION, DataType.INT, new DataType[] { DataType.INT });
    catalog.registerFunction(funcMeta);

    funcMeta = new FunctionDesc("aggmax", Aggmax.class,
        FunctionType.AGGREGATION, DataType.INT, new DataType[] { DataType.INT });
    catalog.registerFunction(funcMeta);

    funcMeta = new FunctionDesc("aggsum", Aggsum.class,
        FunctionType.AGGREGATION, DataType.INT, new DataType[] { DataType.INT });
    catalog.registerFunction(funcMeta);

    funcMeta = new FunctionDesc("aggavg", Aggavg.class,
        FunctionType.AGGREGATION, DataType.INT, new DataType[] { DataType.INT });
    catalog.registerFunction(funcMeta);

    funcMeta = new FunctionDesc("aggcount", Aggcount.class,
        FunctionType.AGGREGATION, DataType.INT, new DataType[] { DataType.INT });
    catalog.registerFunction(funcMeta);

    QueryContext ctx = factory.create();

    QueryAnalyzer qa = new QueryAnalyzer(catalog);
    QueryBlock block = (QueryBlock) qa.parse(ctx, QUERIES[0]);
    LogicalNode plan = LogicalPlanner.createPlan(ctx, block);
    // System.out.println(plan.toString());
    LogicalOptimizer.optimize(ctx, plan);

    LogicalRootNode root = (LogicalRootNode) plan;
    GroupbyNode groupByNode = (GroupbyNode) root.getSubNode();

    gnode = groupByNode;
    inputpath = new String("origin");
    outputpath = new String("cuboid");
    /* test input generate end */
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownMiniZKCluster();
    Delete.delete();
  }

  //@Test
  public void test() throws IOException, InterruptedException,
      ExecutionException {
    Cons.gnode = gnode;

    Cons.groupnum = Cons.gnode.getGroupingColumns().length;
    Cons.measurenum = Cons.gnode.getTargetList().length - Cons.groupnum;
    Cons.totalcuboids = SomeFunctions.power(2, Cons.groupnum);
    // Cons.cubenum = Cons.totalcuboids - 1;

    CubeConf conf;

    conf = new CubeConf();
    conf.setInschema(Cons.gnode.getInputSchema());
    conf.setOutschema(Cons.gnode.getOutputSchema());
    conf.setLocalInput(inputpath);
    // conf.setGlobalOutput(new String("cuboid" + Cons.cubenum));
    conf.setNodenum(0);

    // LocalEngn l1 = new LocalEngn();
    // l1.run(conf);

    testThread t1 = (new TestCube()).new testThread(conf);
    t1.start();

    // conf = new CubeConf();
    // conf.setInschema(Cons.gnode.getInputSchema().toSchema());
    // conf.setOutschema(Cons.gnode.getOutputSchema().toSchema());
    // conf.setLocalInput(inputpath);
    // conf.setGlobalOutput(new String("cuboid" + Cons.cubenum));
    // conf.setNodenum(1);
    // testThread t2 = (new TestCube()).new testThread(conf);
    // t2.start();

    // LocalEngn l2 = new LocalEngn();
    // l2.run(conf);

    System.out.println("server start");

    Cons.totalnodes = 1;

    conf = new CubeConf();
    conf.setInschema(Cons.gnode.getOutputSchema());
    conf.setOutschema(Cons.gnode.getOutputSchema());
    // conf.setLocalInput(new String("origin"));
    conf.setGlobalOutput(outputpath);
    conf.setNodenum(2);
    ServerEngn se = new ServerEngn();
    se.run(conf);
    
    System.out.println("server fin");

    t1.stop();
    // t2.stop();
    System.out.println("server fin");
    
    File f = new File(TestCubeSchema.datapath+"/cuboid/data/node2");
    FileInputStream fstream = new FileInputStream(f);
    DataInputStream in = new DataInputStream(fstream);
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String strLine;
    
    String[] result= {
        "1,150,3",
        "0,200,4",
        "2,150,3"
        };
    
    int rs = 0;
    while ((strLine = br.readLine()) != null)   {
      assertEquals(strLine,result[rs]);
      rs++;
    }

  }

  public class testThread extends Thread {
    CubeConf conf = new CubeConf();

    public testThread(CubeConf conf) {
      this.conf = conf;
    }

    public void run() {
      LocalEngn le = new LocalEngn();
      try {
        System.out.println("thread" + conf.getNodenum() + " start");
        le.run(conf);
        System.out.println("thread" + conf.getNodenum() + " end");
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }
  }

}
