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
 * See the License for the specific language governing permissions WHERE
 * limitations under the License.
 */

package org.apache.tajo;

import org.apache.tajo.conf.TajoConf;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TestHashThetaJoin extends QueryTestCaseBase {

  public TestHashThetaJoin() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);

    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname,
        TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname,
        TajoConf.ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(
        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
        TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(
        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname, String.valueOf(256 * 1048576));
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        String.valueOf(256 * 1048576));
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
        String.valueOf(256 * 1048576));
  }

  @Test
  @SimpleTest(withExplain = true, queries = {
      "select n_nationkey, r_regionkey from nation join region on n_nationkey >= r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation join region on n_nationkey <= r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation join region on n_nationkey > r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation join region on n_nationkey < r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation join region on n_nationkey != r_regionkey WHERE n_nationkey < 7;"})
  public final void testThetaJoinInner() throws Exception {
    runSimpleTests();
  }

  @Test
  @SimpleTest(withExplain = true, queries = {
      "select n_nationkey, r_regionkey from nation left join region on n_nationkey >= r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation left join region on n_nationkey <= r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation left join region on n_nationkey > r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation left join region on n_nationkey < r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation left join region on n_nationkey != r_regionkey WHERE n_nationkey < 7;"})
  public final void testThetaJoinLeftOuter() throws Exception {
    runSimpleTests();
  }

  @Test
  @SimpleTest(withExplain = true, queries = {
      "select n_nationkey, r_regionkey from nation right join region on n_nationkey >= r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation right join region on n_nationkey <= r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation right join region on n_nationkey > r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation right join region on n_nationkey < r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation right join region on n_nationkey != r_regionkey WHERE n_nationkey < 7;"})
  public final void testThetaJoinRightOuter() throws Exception {
    runSimpleTests();
  }

  @Test
  @SimpleTest(withExplain = true, queries = {
      "select n_nationkey, r_regionkey from nation full join region on n_nationkey >= r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation full join region on n_nationkey <= r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation full join region on n_nationkey > r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation full join region on n_nationkey < r_regionkey WHERE n_nationkey < 7;",
      "select n_nationkey, r_regionkey from nation full join region on n_nationkey != r_regionkey WHERE n_nationkey < 7;"})
  public final void testThetaJoinFullOuter() throws Exception {
    runSimpleTests();
  }

}
