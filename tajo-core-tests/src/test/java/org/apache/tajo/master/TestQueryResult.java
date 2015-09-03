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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.junit.Test;

import java.sql.ResultSet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestQueryResult extends QueryTestCaseBase {

  public TestQueryResult() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public final void testTemporalResultOnClose() throws Exception {

    ResultSet res = executeString("select l_orderkey, l_partkey from lineitem where 1=1;");
    QueryId queryId = getQueryId(res);
    Path resultPath = TajoConf.getTemporalResultDir(testingCluster.getConfiguration(), queryId);
    assertTrue(testingCluster.getDefaultFileSystem().exists(resultPath));

    assertResultSet(res);
    cleanupQuery(res);
    assertFalse(testingCluster.getDefaultFileSystem().exists(resultPath));
  }

  @Test
  public final void testTableResultOnClose() throws Exception {

    ResultSet res = executeString("select * from lineitem limit 1");
    QueryId queryId = getQueryId(res);
    Path resultPath = TajoConf.getTemporalResultDir(testingCluster.getConfiguration(), queryId);
    assertFalse(testingCluster.getDefaultFileSystem().exists(resultPath));
    assertResultSet(res);
    cleanupQuery(res);
  }
}
