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

package org.apache.tajo.util.metrics.reporter;

import com.codahale.metrics.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class MetricsConsoleReporter extends TajoMetricsReporter {
  @Override
  public void report(SortedMap<String, Gauge> gauges,
                     SortedMap<String, Counter> counters,
                     SortedMap<String, Histogram> histograms,
                     SortedMap<String, Meter> meters,
                     SortedMap<String, Timer> timers) {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    final String dateTime = dateFormat.format(new Date());
    double rateFactor = TimeUnit.SECONDS.toSeconds(1);

    if (!gauges.isEmpty()) {
      Map<String, Map<String, Gauge>> gaugeGroups = findMetricsItemGroup(gauges);

      for(Map.Entry<String, Map<String, Gauge>> eachGroup: gaugeGroups.entrySet()) {
        System.out.println(gaugeGroupToString(dateTime, null, rateFactor, eachGroup.getKey(), eachGroup.getValue()));
      }
    }

    if (!counters.isEmpty()) {
      Map<String, Map<String, Counter>> counterGroups = findMetricsItemGroup(counters);

      for(Map.Entry<String, Map<String, Counter>> eachGroup: counterGroups.entrySet()) {
        System.out.println(counterGroupToString(dateTime, null, rateFactor, eachGroup.getKey(), eachGroup.getValue()));
      }
    }

    if (!histograms.isEmpty()) {
      Map<String, Map<String, Histogram>> histogramGroups = findMetricsItemGroup(histograms);

      for(Map.Entry<String, Map<String, Histogram>> eachGroup: histogramGroups.entrySet()) {
        System.out.println(histogramGroupToString(dateTime, null, rateFactor, eachGroup.getKey(), eachGroup.getValue()));
      }
    }

    if (!meters.isEmpty()) {
      Map<String, Map<String, Meter>> meterGroups = findMetricsItemGroup(meters);

      for(Map.Entry<String, Map<String, Meter>> eachGroup: meterGroups.entrySet()) {
        System.out.println(meterGroupToString(dateTime, null, rateFactor, eachGroup.getKey(), eachGroup.getValue()));
      }
    }

    if (!timers.isEmpty()) {
      Map<String, Map<String, Timer>> timerGroups = findMetricsItemGroup(timers);

      for(Map.Entry<String, Map<String, Timer>> eachGroup: timerGroups.entrySet()) {
        System.out.println(timerGroupToString(dateTime, null, rateFactor, eachGroup.getKey(), eachGroup.getValue()));
      }
    }
  }
}
