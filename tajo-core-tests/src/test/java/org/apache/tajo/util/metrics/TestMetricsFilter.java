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

package org.apache.tajo.util.metrics;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestMetricsFilter {
  @Test
  public void testGroupNameMetricsFilter() {
    GroupNameMetricsFilter filter = new GroupNameMetricsFilter("tajomaster");

    assertTrue(filter.matches("tajomaster.JVM.Heap.memFree", null));
    assertTrue(!filter.matches("tajomaster01.JVM.Heap.memFree", null));
    assertTrue(!filter.matches("server.tajomaster.JVM.Heap.memFree", null));
    assertTrue(!filter.matches("tajworker.JVM.Heap.memFree", null));
  }

  @Test
  public void testRegexpMetricsFilter() {
    List<String> filterExpressions = new ArrayList<>();
    filterExpressions.add("JVM");
    filterExpressions.add("Query");

    RegexpMetricsFilter filter = new RegexpMetricsFilter(filterExpressions);

    assertTrue(filter.matches("tajomaster.JVM.Heap.memFree", null));
    assertTrue(filter.matches("tajomaster.Query.numQuery", null));

    assertTrue(!filter.matches("tajomaster.resource.numWorker", null));
  }
}
