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
import org.apache.tajo.exception.NoSuchSessionVariableException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Category(IntegrationTest.class)
public class TestIndexScan extends QueryTestCaseBase {

  public TestIndexScan() throws ServiceException, SQLException, NoSuchSessionVariableException {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
    Map<String,String> sessionVars = new HashMap<>();
    sessionVars.put(SessionVars.INDEX_ENABLED.keyname(), "true");
    sessionVars.put(SessionVars.INDEX_SELECTIVITY_LIMIT.keyname(), "0.01f");
    client.updateSessionVariables(sessionVars);
  }

  @Test
  public final void testOnSortedNonUniqueKeys() throws Exception {
    executeString("create index l_orderkey_idx on lineitem (l_orderkey)");
    try {
      ResultSet res = executeString("select * from lineitem where l_orderkey = 1;");
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("drop index l_orderkey_idx");
    }
  }

  @Test
  public final void testOnUnsortedTextKeys() throws Exception {
    executeString("create index l_shipdate_idx on lineitem (l_shipdate)");
    try {
      ResultSet res = executeString("select l_orderkey, l_shipdate, l_comment from lineitem where l_shipdate = '1997-01-28';");
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("drop index l_shipdate_idx");
    }
  }

  @Test
  public final void testOnMultipleKeys() throws Exception {
    executeString("create index multikey_idx on lineitem (l_shipdate asc nulls last, l_tax desc nulls first, l_shipmode, l_linenumber desc nulls last)");
    try {
      ResultSet res = executeString("select l_orderkey, l_shipdate, l_comment from lineitem " +
          "where l_shipdate = '1997-01-28' and l_tax = 0.05 and l_shipmode = 'RAIL' and l_linenumber = 1;");
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("drop index multikey_idx");
    }
  }

  @Test
  public final void testOnMultipleKeys2() throws Exception {
    executeString("create index multikey_idx on lineitem (l_shipdate asc nulls last, l_tax desc nulls first)");
    try {
      ResultSet res = executeString("select l_orderkey, l_shipdate, l_comment from lineitem " +
          "where l_shipdate = '1997-01-28' and l_tax = 0.05 and l_shipmode = 'RAIL' and l_linenumber = 1;");
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("drop index multikey_idx");
    }
  }

  @Test
  public final void testOnMultipleExprs() throws Exception {
    executeString("create index l_orderkey_100_l_linenumber_10_idx on lineitem (l_orderkey*100-l_linenumber*10 asc nulls first);");
    try {
      ResultSet res = executeString("select l_orderkey, l_linenumber from lineitem where l_orderkey*100-l_linenumber*10 = 280");
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("drop index l_orderkey_100_l_linenumber_10_idx");
    }
  }

  @Test
  public final void testWithGroupBy() throws Exception {
    executeString("create index l_shipdate_idx on lineitem (l_shipdate)");
    try {
      ResultSet res = executeString("select l_shipdate, count(*) from lineitem where l_shipdate = '1997-01-28' group by l_shipdate;");
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("drop index l_shipdate_idx");
    }
  }

  @Test
  public final void testWithSort() throws Exception {
    executeString("create index l_orderkey_idx on lineitem (l_orderkey)");
    try {
      ResultSet res = executeString("select l_shipdate from lineitem where l_orderkey = 1 order by l_shipdate;");
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("drop index l_orderkey_idx");
    }
  }

  @Test
  public final void testWithJoin() throws Exception {
    executeString("create index l_orderkey_idx on lineitem (l_orderkey)");
    executeString("create index o_orderkey_idx on orders (o_orderkey)");
    try {
      ResultSet res = executeString("select l_shipdate, o_orderstatus from lineitem, orders where l_orderkey = o_orderkey and l_orderkey = 1 and o_orderkey = 1;");
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("drop index l_orderkey_idx");
      executeString("drop index o_orderkey_idx");
    }
  }
}
