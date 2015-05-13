///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.tajo.engine.query;
//
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.tajo.*;
//import org.apache.tajo.catalog.*;
//import org.apache.tajo.common.TajoDataTypes.Type;
//import org.apache.tajo.conf.TajoConf;
//import org.apache.tajo.datum.Datum;
//import org.apache.tajo.datum.Int4Datum;
//import org.apache.tajo.datum.TextDatum;
//import org.apache.tajo.engine.planner.global.ExecutionBlock;
//import org.apache.tajo.engine.planner.global.MasterPlan;
//import org.apache.tajo.jdbc.FetchResultSet;
//import org.apache.tajo.plan.logical.NodeType;
//import org.apache.tajo.querymaster.QueryMasterTask;
//import org.apache.tajo.storage.*;
//import org.apache.tajo.util.FileUtil;
//import org.apache.tajo.util.KeyValueSet;
//import org.apache.tajo.worker.TajoWorker;
//import org.junit.AfterClass;
//import org.junit.Test;
//import org.junit.experimental.categories.Category;
//import org.junit.runner.RunWith;
//import org.junit.runners.Parameterized;
//
//import java.io.File;
//import java.io.OutputStream;
//import java.sql.ResultSet;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.List;
//
//import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
//import static org.junit.Assert.*;
//
//@Category(IntegrationTest.class)
//@RunWith(Parameterized.class)
//public class TestJoinBroadcast extends QueryTestCaseBase {
//  public TestJoinBroadcast(String joinOption) throws Exception {
//    super(TajoConstants.DEFAULT_DATABASE_NAME);
//    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname, "true");
//    testingCluster.setAllTajoDaemonConfValue(
//        TajoConf.ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname, "" + (5 * 1024));
//
//    executeDDL("create_lineitem_large_ddl.sql", "lineitem_large");
//    executeDDL("create_customer_large_ddl.sql", "customer_large");
//    executeDDL("create_orders_large_ddl.sql", "orders_large");
//
//    testingCluster.setAllTajoDaemonConfValue(
//        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
//        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);
//
//    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
//        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);
//    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
//        TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.defaultVal);
//
//    if (joinOption.indexOf("Hash") >= 0) {
//      testingCluster.setAllTajoDaemonConfValue(
//          TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname, String.valueOf(256 * 1048576));
//      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
//          String.valueOf(256 * 1048576));
//      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
//          String.valueOf(256 * 1048576));
//    }
//    if (joinOption.indexOf("Sort") >= 0) {
//      testingCluster.setAllTajoDaemonConfValue(
//          TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname, String.valueOf(1));
//      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
//          String.valueOf(1));
//      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
//          String.valueOf(1));
//    }
//  }
//
//  @Parameterized.Parameters
//  public static Collection<Object[]> generateParameters() {
//    return Arrays.asList(new Object[][]{
//        {"Hash"},
//        {"Sort"},
//    });
//  }
//
//  @AfterClass
//  public static void classTearDown() {
//    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname,
//        TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.defaultVal);
//    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname,
//        TajoConf.ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.defaultVal);
//
//    testingCluster.setAllTajoDaemonConfValue(
//        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
//        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);
//
//    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
//        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);
//    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
//        TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.defaultVal);
//  }
//
//
//
//
//}
