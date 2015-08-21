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

package org.apache.tajo.benchmark;

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TestTPCH extends QueryTestCaseBase {

  public TestTPCH() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true)
  @SimpleTest
  public void testQ1OrderBy() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true)
  @SimpleTest
  public void testQ2FourJoins() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true)
  @SimpleTest
  public void testTPCH14Expr() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true)
  @SimpleTest
  public void testTPCHQ5() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true)
  @SimpleTest
  public void testFirstJoinInQ7() throws Exception {
    runSimpleTests();
  }

  @Test
  public void testMelon() throws Exception {
    try {
      executeString("create table partitioned_lineitem (l_orderkey int8, l_partkey int8, l_suppkey int8, l_linenumber int8, l_quantity float8, l_extendedprice float8, l_discount float8, l_tax float8, l_linestatus text, l_shipdate text, l_commitdate text, l_receiptdate text, l_shipinstruct text, l_shipmode text, l_comment text) partition by column (l_returnflag text) as select l_orderkey, l_partkey, l_suppkey, l_linenumber , l_quantity , l_extendedprice , l_discount , l_tax , l_linestatus , l_shipdate , l_commitdate , l_receiptdate , l_shipinstruct , l_shipmode , l_comment, l_returnflag from lineitem");

      executeString("select l_linenumber, sum(sum_val) from ( select l_linenumber, sum(l_discount) sum_val from (select l_linenumber, l_discount, s_nationkey from partitioned_lineitem a1 left outer join supplier a2 on s_suppkey = l_suppkey where a2.s_suppkey is not null) t1 inner join nation t2 on t2.n_nationkey = t1.s_nationkey group by l_linenumber union select l_linenumber, sum(l_discount) sum_val from (select l_linenumber, l_discount, s_nationkey from partitioned_lineitem a1 left outer join supplier a2 on s_suppkey = l_suppkey where a2.s_suppkey is not null) t3 inner join nation t4 on t4.n_nationkey = t3.s_nationkey group by l_linenumber ) sx_std group by l_linenumber;");
    } finally {
      executeString("drop table partitioned_lineitem purge");

    }
  }
}