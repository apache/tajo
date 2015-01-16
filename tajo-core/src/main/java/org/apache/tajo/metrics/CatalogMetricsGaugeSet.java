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

package org.apache.tajo.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import org.apache.tajo.master.TajoMaster;

import java.util.HashMap;
import java.util.Map;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;

public class CatalogMetricsGaugeSet implements MetricSet {
  TajoMaster.MasterContext tajoMasterContext;
  public CatalogMetricsGaugeSet(TajoMaster.MasterContext tajoMasterContext) {
    this.tajoMasterContext = tajoMasterContext;
  }

  @Override
  public Map<String, Metric> getMetrics() {
    Map<String, Metric> metricsMap = new HashMap<String, Metric>();
    metricsMap.put("numTables", new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return tajoMasterContext.getCatalog().getAllTableNames(DEFAULT_DATABASE_NAME).size();
      }
    });

    metricsMap.put("numFunctions", new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return tajoMasterContext.getCatalog().getFunctions().size();
      }
    });

    return metricsMap;
  }
}
