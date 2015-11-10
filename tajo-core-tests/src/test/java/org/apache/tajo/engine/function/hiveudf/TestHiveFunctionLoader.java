/***
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

package org.apache.tajo.engine.function.hiveudf;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.net.URL;
import java.util.*;

public class TestHiveFunctionLoader {
  private TajoConf conf = new TajoConf();

  private CatalogService catService;
  private TajoTestingCluster cluster;

  @Before
  public final void setUp() throws Exception {
    LocalTajoTestingUtility util = new LocalTajoTestingUtility();
    cluster = new TajoTestingCluster();
    cluster.startCatalogCluster();
    catService = cluster.getCatalogService();

    URL hiveUDFURL = ClassLoader.getSystemResource("hiveudf");
    Preconditions.checkNotNull(hiveUDFURL, "hive udf directory is absent.");

    conf.set("hive.udf.dir", hiveUDFURL.toString().substring("file:".length()));

    HiveFunctionLoader.loadHiveUDFs(conf);
  }

  @After
  public final void tearDown() {
    cluster.shutdownCatalogCluster();
  }

  @Test
  public void testAnalyzeUDFclass() {
    Set<Class<? extends UDF>> funcSet = new HashSet<>();
    funcSet.add(HiveUDFtest.class);
    List<FunctionDesc> funcList = new LinkedList<>();

    HiveFunctionLoader.analyzeUDFclasses(funcSet, funcList);

    assertEquals(funcList.size(), 1);

    FunctionDesc desc = funcList.get(0);

    assertEquals(desc.getFunctionName(), "multiplestr");
    assertEquals(desc.isDeterministic(), false);
    assertEquals(desc.getReturnType().getType(), TajoDataTypes.Type.TEXT);
    assertEquals(desc.getParamTypes()[0].getType(), TajoDataTypes.Type.TEXT);
    assertEquals(desc.getParamTypes()[1].getType(), TajoDataTypes.Type.INT4);
  }
}