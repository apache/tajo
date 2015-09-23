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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

import java.util.HashMap;
import java.util.Map;

public class LogEventGaugeSet implements MetricSet {

  @Override
  public Map<String, Metric> getMetrics() {
    final Map<String, Metric> gauges = new HashMap<>();

    gauges.put("Fatal", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return TajoLogEventCounter.getFatal();
      }
    });

    gauges.put("Error", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return TajoLogEventCounter.getError();
      }
    });

    gauges.put("Warn", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return TajoLogEventCounter.getWarn();
      }
    });

    gauges.put("Info", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return TajoLogEventCounter.getInfo();
      }
    });

    return gauges;
  }
}
