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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class RegexpMetricsFilter implements MetricFilter {
  List<Pattern> filterPatterns = new ArrayList<>();

  public RegexpMetricsFilter(Collection<String> filterExpressions) {
    filterPatterns.addAll(filterExpressions.stream().map(Pattern::compile).collect(Collectors.toList()));
  }

  @Override
  public boolean matches(String name, Metric metric) {
    if(filterPatterns.isEmpty()) {
      return true;
    }

    for(Pattern eachPattern: filterPatterns) {
      if(eachPattern.matcher(name).find()) {
        return true;
      }
    }
    return false;
  }
}
