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

import com.google.protobuf.ServiceException;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.NamedTest;
import org.apache.tajo.conf.TajoConf;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
@NamedTest("TestJoinQuery")
public class TestOuterJoinWithSubQuery extends TestJoinQuery {

  public TestOuterJoinWithSubQuery(String joinOption) throws Exception {
    super(joinOption);
  }

  @BeforeClass
  public static void setup() throws Exception {
    TestJoinQuery.setup();
  }

  @AfterClass
  public static void classTearDown() throws ServiceException {
    TestJoinQuery.classTearDown();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoinWithConstantExpr2() throws Exception {
    // outer join with constant projections
    //
    // select c_custkey, o.o_orderkey, 'val' as val from customer left outer join
    // (select * from orders) o on c_custkey = o.o_orderkey
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public final void testLeftOuterJoinWithConstantExpr3() throws Exception {
    // outer join with constant projections
    //
    // select a.c_custkey, 123::INT8 as const_val, b.min_name from customer a
    // left outer join ( select c_custkey, min(c_name) as min_name from customer group by c_custkey) b
    // on a.c_custkey = b.c_custkey;
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest(queries = {
      @QuerySpec("select a.id, b.id from jointable11 a " +
          "left outer join (" +
          "select jointable12.id from jointable12 inner join lineitem " +
          "on jointable12.id = lineitem.l_orderkey and jointable12.id > 10) b " +
          "on a.id = b.id order by a.id")
  })
  public final void testLeftOuterJoinWithEmptySubquery1() throws Exception {
    try {
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$TEST_MIN_TASK_NUM.varname, "2");
      runSimpleTests();
    } finally {
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$TEST_MIN_TASK_NUM.varname,
          TajoConf.ConfVars.$TEST_MIN_TASK_NUM.defaultVal);
    }
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest(queries = {
      @QuerySpec("select a.id, b.id from " +
          "(select jointable12.id, jointable12.name, lineitem.l_shipdate " +
          "from jointable12 inner join lineitem on jointable12.id = lineitem.l_orderkey and jointable12.id > 10) a " +
          "left outer join jointable11 b on a.id = b.id")
  })
  public final void testLeftOuterJoinWithEmptySubquery2() throws Exception {
    //Empty Preserved Row table
    try {
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$TEST_MIN_TASK_NUM.varname, "2");
      runSimpleTests();
    } finally {
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$TEST_MIN_TASK_NUM.varname,
          TajoConf.ConfVars.$TEST_MIN_TASK_NUM.defaultVal);
    }
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest(queries = {
      @QuerySpec("select a.l_orderkey \n" +
          "from (select * from lineitem where l_orderkey < 0) a\n" +
          "full outer join (select * from lineitem where l_orderkey < 0) b\n" +
          "on a.l_orderkey = b.l_orderkey")
  })
  public void testFullOuterJoinWithEmptyIntermediateData() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest(queries = {
      @QuerySpec("select count(b.id) " +
          "from (select id, count(*) as cnt from jointable_large group by id) a " +
          "left outer join (select id, count(*) as cnt from jointable_large where id < 200 group by id) b " +
          "on a.id = b.id")
  })
  public void testJoinWithDifferentShuffleKey() throws Exception {
    int originConfValue = conf.getIntVar(TajoConf.ConfVars.$DIST_QUERY_JOIN_PARTITION_VOLUME);
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$DIST_QUERY_JOIN_PARTITION_VOLUME.varname, "1");
    try {
      runSimpleTests();
    } finally {
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$DIST_QUERY_JOIN_PARTITION_VOLUME.varname,
          "" + originConfValue);
    }
  }
}
