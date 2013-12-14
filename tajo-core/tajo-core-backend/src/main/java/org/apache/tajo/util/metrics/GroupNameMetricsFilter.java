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

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;

public class GroupNameMetricsFilter implements MetricFilter {
  String groupName;

  public GroupNameMetricsFilter(String groupName) {
    this.groupName = groupName;
  }
  @Override
  public boolean matches(String name, Metric metric) {
    if(name != null) {
      String[] tokens = name.split("\\.");
      if(groupName.equals(tokens[0])) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }
}
