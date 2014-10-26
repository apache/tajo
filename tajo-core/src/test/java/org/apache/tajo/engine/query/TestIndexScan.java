/*
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

package org.apache.tajo.engine.query;

import com.google.protobuf.ServiceException;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoConstants;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;
import java.util.Map;

@Category(IntegrationTest.class)
public class TestIndexScan extends QueryTestCaseBase {

  public TestIndexScan() throws ServiceException {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
    Map<String,String> sessionVars = client.getAllSessionVariables();
    sessionVars.put(SessionVars.INDEX_ENABLED.keyname(), "true");
    sessionVars.put(SessionVars.INDEX_SELECTIVITY_THRESHOLD.keyname(), "0.01f");
    client.updateSessionVariables(sessionVars);
  }

  @Test
  public final void testOnSortedNonUniqueKeys() throws Exception {
    executeString("create index l_orderkey_idx on lineitem (l_orderkey)");
    ResultSet res = executeString("select * from lineitem where l_orderkey = 1;");
    assertResultSet(res);
    cleanupQuery(res);
    executeString("drop index l_orderkey_idx");
  }

  @Test
  public final void testOnUnsortedTextKeys() throws Exception {
    executeString("create index l_shipdate_idx on lineitem (l_shipdate)");
    ResultSet res = executeString("select l_orderkey, l_shipdate, l_comment from lineitem where l_shipdate = '1997-01-28';");
    assertResultSet(res);
    cleanupQuery(res);
    executeString("drop index l_shipdate_idx");
  }
}
