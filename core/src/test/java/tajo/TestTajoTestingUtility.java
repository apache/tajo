/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package tajo;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.FunctionType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.planner.LogicalOptimizer;
import tajo.engine.planner.LogicalPlanner;
import tajo.engine.planner.PlanningContext;
import tajo.engine.planner.global.QueryUnit;
import tajo.engine.planner.global.QueryUnitAttempt;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.query.QueryUnitRequestImpl;
import tajo.ipc.protocolrecords.Fragment;
import tajo.ipc.protocolrecords.QueryUnitRequest;
import tajo.master.Query;
import tajo.master.SubQuery;
import tajo.storage.Appender;
import tajo.storage.StorageManager;
import tajo.storage.Tuple;
import tajo.storage.VTuple;
import tajo.worker.SlowFunc;
import tajo.worker.Worker;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;

public class TestTajoTestingUtility {
  private TajoTestingUtility util;
  private int num = 4;
  private CatalogService catalog;
  private TajoConf conf;
  private StorageManager sm;
  private QueryAnalyzer analyzer;
  private LogicalPlanner planner;
  private int tupleNum = 10000;

  @Before
  public void setUp() throws Exception {
    util = new TajoTestingUtility();
    util.startMiniCluster(num);
          
    catalog = util.getMiniTajoCluster().getMaster().getCatalog();
    conf = util.getConfiguration();
    sm = StorageManager.get(conf);
    QueryIdFactory.reset();
    analyzer = new QueryAnalyzer(catalog);
    planner = new LogicalPlanner(catalog);
    
    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("empId", DataType.INT);
    schema.addColumn("deptName", DataType.STRING);
    TableMeta employeeMeta = TCatUtil.newTableMeta(schema, StoreType.CSV);    
    sm.initTableBase(employeeMeta, "employee");

    Appender appender = sm.getAppender(employeeMeta, "employee", "employee");
    Tuple tuple = new VTuple(employeeMeta.getSchema().getColumnNum());

    for (int i = 0; i < tupleNum; i++) {
      tuple.put(new Datum[] {
          DatumFactory.createString("name_" + i),
          DatumFactory.createInt(i),
          DatumFactory.createString("dept_" + i)});
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();
    
    TableDesc desc = TCatUtil.newTableDesc("employee", employeeMeta, 
        sm.getTablePath("employee")); 
    catalog.addTable(desc);
    FunctionDesc func = new FunctionDesc("sleep", SlowFunc.class, FunctionType.AGGREGATION,
        new DataType[] {DataType.STRING}, new DataType [] {DataType.STRING});
    catalog.registerFunction(func);
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public final void test() throws Exception {
    Fragment[] frags = sm.split("employee", 40000);
    int splitIdx = (int) Math.ceil(frags.length / 2.f);
    QueryIdFactory.reset();
    QueryId queryId = QueryIdFactory.newQueryId();
    SubQueryId subQueryId = QueryIdFactory.newSubQueryId(queryId);
    Query query = new Query(queryId,
        "testNtaTestingUtil := select deptName, sleep(name) from employee group by deptName");
    SubQuery subQuery = new SubQuery(subQueryId);
    query.addSubQuery(subQuery);
    util.getMiniTajoCluster().getMaster().getQueryManager().addQuery(query);

    QueryUnitId qid;
    LogicalNode plan;
    QueryUnitRequest req;
    Thread.sleep(2000);

    sm.initTableBase(frags[0].getMeta(), "testNtaTestingUtil");

    List<QueryUnit> queryUnits = Lists.newArrayList();
    List<QueryUnitRequest> queryUnitRequests = Lists.newArrayList();
    for (int i = 0; i < 4; i++) {
      qid = QueryIdFactory.newQueryUnitId(subQueryId);
      PlanningContext context = analyzer.parse(
          "testNtaTestingUtil := select deptName, sleep(name) from employee group by deptName");
      plan = planner.createPlan(context);
      plan = LogicalOptimizer.optimize(context, plan);
      QueryUnit unit = new QueryUnit(qid);
      queryUnits.add(unit);
      QueryUnitAttempt attempt = unit.newAttempt();
      req = new QueryUnitRequestImpl(
          attempt.getId(),
          Lists.newArrayList(Arrays.copyOfRange(frags, 0, splitIdx)),
          "", false, plan.toJSON());
      queryUnitRequests.add(req);
    }
    subQuery.setQueryUnits(queryUnits.toArray(new QueryUnit[queryUnits.size()]));

    for (int i = 0; i < 4; i++) {
      util.getMiniTajoCluster().getWorkerThreads().get(i)
          .getWorker().requestQueryUnit(queryUnitRequests.get(i).getProto());
    }


    Thread.sleep(3000);
    Worker leaf0 = util.getMiniTajoCluster().getWorker(0);
    leaf0.shutdown("Aborted!");

    Thread.sleep(1000);
    Worker leaf1 = util.getMiniTajoCluster().getWorker(1);
    leaf1.shutdown("Aborted!");

    Thread.sleep(1000);
    Worker leaf2 = util.getMiniTajoCluster().getWorker(2);
    leaf2.shutdown("Aborted!");

    Thread.sleep(1000);
    Worker leaf3 = util.getMiniTajoCluster().getWorker(3);
    leaf3.shutdown("Aborted!");

    assertFalse(leaf0.isAlive());
    assertFalse(leaf1.isAlive());
    assertFalse(leaf2.isAlive());
    assertFalse(leaf3.isAlive());
  }
}
