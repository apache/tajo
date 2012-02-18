///**
// * 
// */
//package nta.engine.planner.global;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import nta.catalog.Column;
//import nta.catalog.Schema;
//import nta.catalog.TableDescImpl;
//import nta.catalog.proto.CatalogProtos.DataType;
//import nta.catalog.proto.CatalogProtos.StoreType;
//import nta.distexec.DistPlan;
//import nta.engine.QueryIdFactory;
//import nta.engine.parser.QueryBlock.FromTable;
//import nta.engine.planner.logical.ExprType;
//import nta.engine.planner.logical.GroupbyNode;
//import nta.engine.planner.logical.LogicalRootNode;
//import nta.engine.planner.logical.ScanNode;
//
//import org.junit.Before;
//import org.junit.Test;
//
//import static org.junit.Assert.*;
//
///**
// * @author jihoon
// * 
// */
//public class TestGlobalOptimizer {
//  GlobalOptimizer optimizer;
//  QueryUnitGraph queryGraph;
//
//  @Before
//  public void setup() {
//    Schema schema = new Schema();
//    QueryIdFactory.reset();
//    optimizer = new GlobalOptimizer();
//    String[] plans = { "merge", "local" };
//    int[] nodeNum = { 1, 3 };
//    MappingType[] mappings = { MappingType.ONE_TO_ONE, MappingType.ONE_TO_ONE };
//    OptimizationPlan plan = new OptimizationPlan(2, plans, nodeNum, mappings);
//    optimizer.addOptimizationPlan(ExprType.GROUP_BY, plan);
//
//    LogicalRootNode rn = new LogicalRootNode();
//    rn.setInputSchema(schema);
//    rn.setOutputSchema(schema);
//
//    GroupbyNode gn = new GroupbyNode(new Column[] { new Column("a",
//        DataType.INT) });
//    gn.setInputSchema(schema);
//    gn.setOutputSchema(schema);
//    rn.setSubNode(gn);
//
//    ScanNode[] sn = new ScanNode[5];
//    for (int i = 0; i < 5; i++) {
//      sn[i] = new ScanNode(new FromTable(new TableDescImpl("test", schema,
//          StoreType.CSV)));
//      sn[i].setInputSchema(schema);
//      sn[i].setOutputSchema(schema);
//      gn.setSubNode(sn[i]);
//    }
//
//    QueryUnit groupby = new QueryUnit(QueryIdFactory.newQueryUnitId());
//    groupby.set(gn, null);
//    QueryUnit root = new QueryUnit(QueryIdFactory.newQueryUnitId());
//    root.set(rn, null);
//    root.addPrevQuery(groupby);
//    groupby.addNextQuery(root);
//
//    QueryUnit[] scans = new QueryUnit[5];
//    for (int i = 0; i < 5; i++) {
//      scans[i] = new QueryUnit(QueryIdFactory.newQueryUnitId());
//      scans[i].set(sn[i], null);
//      groupby.addPrevQuery(scans[i]);
//      scans[i].addNextQuery(groupby);
//    }
//
//    queryGraph = new QueryUnitGraph(root);
//  }
//
//  @Test
//  public void test() {
//    QueryUnitGraph optimizedGraph = optimizer.optimize(queryGraph);
//    List<QueryUnit> q = new ArrayList<QueryUnit>();
//    QueryUnit query = optimizedGraph.getRoot();
//    assertEquals(ExprType.ROOT, query.getOp().getType());
//    assertEquals(1, query.getPrevQueries().size());
//
//    query = query.getPrevQueries().iterator().next();
//    assertEquals(ExprType.GROUP_BY, query.getOp().getType());
//    DistPlan plan = query.getDistPlan();
//    assertEquals("merge", plan.getPlanName());
//    assertEquals(1, plan.getOutputNum());
//    assertEquals(3, query.getPrevQueries().size());
//
//    for (QueryUnit uq : query.getPrevQueries()) {
//      assertEquals(ExprType.GROUP_BY, uq.getOp().getType());
//      plan = uq.getDistPlan();
//      assertEquals("local", plan.getPlanName());
//      assertEquals(1, plan.getOutputNum());
//      assertTrue(uq.getPrevQueries().size() > 0);
//
//      for (QueryUnit scan : uq.getPrevQueries()) {
//        assertEquals(ExprType.SCAN, scan.getOp().getType());
//      }
//    }
//  }
//}
