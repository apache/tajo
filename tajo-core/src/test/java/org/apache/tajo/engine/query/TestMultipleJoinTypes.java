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

package org.apache.tajo.engine.query;

import org.apache.tajo.NamedTest;
import org.apache.tajo.QueryTestCaseBase;
import org.junit.Test;

import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;

@NamedTest("TestJoinQuery")
public class TestMultipleJoinTypes extends TestJoinQuery {

  public TestMultipleJoinTypes(String joinOption) throws Exception {
    super(joinOption);
  }

  @Test
  @QueryTestCaseBase.Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @QueryTestCaseBase.SimpleTest()
  public final void testJoinWithMultipleJoinTypes() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinsWithCaseWhen() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinsWithCaseWhen2() throws Exception {
    runSimpleTests();
  }

  @Test
  public final void testInnerAndOuterWithEmpty() throws Exception {
    executeDDL("customer_partition_ddl.sql", null);
    executeFile("insert_into_customer_partition.sql").close();

    // outer join table is empty
    ResultSet res = executeString(
        "select a.l_orderkey, b.o_orderkey, c.c_custkey from lineitem a " +
            "inner join orders b on a.l_orderkey = b.o_orderkey " +
            "left outer join customer_broad_parts c on a.l_orderkey = c.c_custkey and c.c_custkey < 0"
    );

    String expected = "l_orderkey,o_orderkey,c_custkey\n" +
        "-------------------------------\n" +
        "1,1,null\n" +
        "1,1,null\n" +
        "2,2,null\n" +
        "3,3,null\n" +
        "3,3,null\n";

    assertEquals(expected, resultSetToString(res));
    res.close();

    executeString("DROP TABLE customer_broad_parts PURGE").close();
  }
}
