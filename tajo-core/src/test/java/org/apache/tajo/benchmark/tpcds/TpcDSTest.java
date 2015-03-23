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

package org.apache.tajo.benchmark.tpcds;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import java.io.File;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

public class TpcDSTest extends QueryTestCaseBase {
  private static final Log LOG = LogFactory.getLog(TpcDSTest.class);

  private static final String TPCDS_DATABASE = "tpcds";

  public TpcDSTest() {
    super(TPCDS_DATABASE);

    try {
      TpcDSTestUtil.createTables(TPCDS_DATABASE, getClient());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      fail(e.getMessage());
    }
  }

  @Test
  public void testAllQueries() throws Exception {
    File resourceDir = new File(ClassLoader.getSystemResource("tpcds").getPath());
    File queryResultParentDir = new File(resourceDir, "results");

    File queryRoot = new File(resourceDir, "/queries");
    String[] queries = FileUtil.readTextFile(new File(queryRoot, "query.list")).split("\\n");

    List<String> successQueries = new ArrayList<String>();
    int numTargetQueries = 0;
    long startTime = System.currentTimeMillis();
    try {
      for (String eachQuery : queries) {
        if (eachQuery.trim().isEmpty() || eachQuery.trim().startsWith("#")) {
          continue;
        }
        numTargetQueries++;
      }

      for (String eachQuery : queries) {
        if (eachQuery.trim().isEmpty() || eachQuery.trim().startsWith("#")) {
          continue;
        }
        String queryId = getQueryId(eachQuery);
        LOG.info("========> Run TPC-DS Query: " + queryId);
        String query = FileUtil.readTextFile(new File(queryRoot, eachQuery));
        long queryStartTime = System.currentTimeMillis();
        ResultSet res = executeString(query);
        try {
          assertEquals("TPC-DS Query: " + queryId + " failed",
              FileUtil.readTextFile(new File(queryResultParentDir, queryId + ".result")), resultSetToString(res));
        } finally {
          res.close();
        }

        successQueries.add(queryId + " (" + (System.currentTimeMillis() - queryStartTime) / 1000 + " sec.)");
      }
    } finally {
      LOG.info("=================================================");
      LOG.info("Test time: " + (System.currentTimeMillis() - startTime) / 1000 + " sec.");
      LOG.info("Succeeded queries: " + successQueries.size() + "/" + numTargetQueries);
      LOG.info("-------------------------------------------------");
      for (String eachQuery: successQueries) {
        System.out.println(eachQuery);
      }
      LOG.info("=================================================");
    }
  }

  private String getQueryId(String queryFileName) {
    String[] tokens = queryFileName.split("/");
    String queryName = tokens[tokens.length - 1];
    return queryName.split("\\.")[0];
  }
}
