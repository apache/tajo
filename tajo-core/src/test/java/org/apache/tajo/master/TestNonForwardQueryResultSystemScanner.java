/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.master;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.File;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.tajo.*;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.client.ResultSetUtil;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.master.exec.NonForwardQueryResultScanner;
import org.apache.tajo.master.exec.NonForwardQueryResultSystemScanner;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.logical.LimitNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.RowStoreUtil.RowStoreDecoder;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.KeyValueSet;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestNonForwardQueryResultSystemScanner extends QueryTestCaseBase {
  @Test
  public void testGetNextRowsForAggregateFunction() throws Exception {
    assertQueryStr("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES " +
        "WHERE TABLE_NAME = 'lineitem' OR TABLE_NAME = 'nation' OR TABLE_NAME = 'customer'");
  }

  @Test
  public void testGetNextRowsForTable() throws Exception {
    assertQueryStr("SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES " +
        "WHERE TABLE_NAME = 'lineitem' OR TABLE_NAME = 'nation' OR TABLE_NAME = 'customer'");
  }

  @Test
  public void testGetClusterDetails() throws Exception {
    assertQueryStr("SELECT TYPE FROM INFORMATION_SCHEMA.CLUSTER");
  }
}
