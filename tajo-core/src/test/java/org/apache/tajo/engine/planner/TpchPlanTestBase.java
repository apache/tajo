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

package org.apache.tajo.engine.planner;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class TpchPlanTestBase {
  private static final Log LOG = LogFactory.getLog(TpchPlanTestBase.class);

  String [] names;
  String [] paths;
  String [][] tables;
  Schema[] schemas;
  Map<String, Integer> nameMap = Maps.newHashMap();
  protected TPCH tpch;
  protected TajoPlanTestingUtility util;

  private static TpchPlanTestBase testBase;

  static {
    try {
      testBase = new TpchPlanTestBase();
      testBase.setUp();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private TpchPlanTestBase() throws IOException {
    names = new String[] {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier", "empty_orders"};
    paths = new String[names.length];
    for (int i = 0; i < names.length; i++) {
      nameMap.put(names[i], i);
    }

    tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadQueries();

    schemas = new Schema[names.length];
    for (int i = 0; i < names.length; i++) {
      schemas[i] = tpch.getSchema(names[i]);
    }

    tables = new String[names.length][];
    File file;
    for (int i = 0; i < names.length; i++) {
      file = TPCH.getDataFile(names[i]);
      tables[i] = FileUtil.readTextFile(file).split("\n");
      paths[i] = file.getAbsolutePath();
    }
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void setUp() throws Exception {
    util = new TajoPlanTestingUtility();
    KeyValueSet opt = new KeyValueSet();
    opt.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    util.setup(names, paths, schemas, opt);
  }

  public static TpchPlanTestBase getInstance() {
    return testBase;
  }

  public LogicalPlan buildLogicalPlan(String query) throws PlanningException {
    return util.buildLogicalPlan(query);
  }

  public MasterPlan buildMasterPlan(LogicalPlan plan) throws IOException, PlanningException {
    return util.buildMasterPlan(plan);
  }

  public TajoPlanTestingUtility getUtilility() {
    return util;
  }

  public void tearDown() throws IOException {
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
    }
    util.shutdown();
  }
}
