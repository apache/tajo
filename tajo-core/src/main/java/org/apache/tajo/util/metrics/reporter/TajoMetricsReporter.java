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

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

public abstract class TajoMetricsReporter {
  public abstract void report(SortedMap<String, Gauge> gauges,
                              SortedMap<String, Counter> counters,
                              SortedMap<String, Histogram> histograms,
                              SortedMap<String, Meter> meters,
                              SortedMap<String, Timer> timers);

  public <T> Map<String, Map<String, T>> findMetricsItemGroup(SortedMap<String, T> metricsMap) {
    Map<String, Map<String, T>> metricsGroup = new HashMap<>();

    String previousGroup = null;
    Map<String, T> groupItems = new HashMap<>();

    for (Map.Entry<String, T> entry : metricsMap.entrySet()) {
      String key = entry.getKey();
      String[] keyTokens = key.split("\\.");

      String groupName = null;
      String itemName = null;

      if (keyTokens.length > 2) {
        groupName = keyTokens[0] + "." + keyTokens[1];
        itemName = "";
        String prefix = "";
        StringBuilder itemNameBuilder = new StringBuilder();
        for (int i = 2; i < keyTokens.length; i++) {
          itemNameBuilder.append(prefix).append(keyTokens[i]);
          prefix = ".";
        }
        itemName = itemNameBuilder.toString();
      } else {
        groupName = "";
        itemName = key;
        if(!metricsGroup.containsKey(groupName)) {
          metricsGroup.put(groupName, new HashMap<String, T>());
        }
        metricsGroup.get(groupName).put(itemName, entry.getValue());
        continue;
      }

      if (previousGroup != null && !previousGroup.equals(groupName)) {
        metricsGroup.put(previousGroup, groupItems);
        groupItems = new HashMap<>();
      }
      groupItems.put(itemName, entry.getValue());
      previousGroup = groupName;
    }

    if(groupItems != null && !groupItems.isEmpty()) {
      metricsGroup.put(previousGroup, groupItems);
    }

    return metricsGroup;
  }

  protected String meterGroupToString(String dateTime, String hostAndPort, double rateFactor,
                                      String groupName, Map<String, Meter> meters) {
    StringBuilder sb = new StringBuilder();
    sb.append(dateTime).append(" ");
    if(hostAndPort != null && !hostAndPort.isEmpty()) {
      sb.append(hostAndPort).append(" ");
    }
    sb.append("meter").append(" ");

    if(!groupName.isEmpty()) {
      sb.append(groupName).append(" ");
    }
    String prefix = "";
    for(Map.Entry<String, Meter> eachMeter: meters.entrySet()) {
      String key = eachMeter.getKey();
      Meter meter = eachMeter.getValue();
      sb.append(prefix);
      sb.append(key).append(".count=").append(meter.getCount()).append("|");
      sb.append(key).append(".mean=").append(String.format("%2.2f",
          convertRate(meter.getMeanRate(), rateFactor))).append("|");
      sb.append(key).append(".1minute=").append(String.format("%2.2f",
          convertRate(meter.getOneMinuteRate(), rateFactor))).append("|");
      sb.append(key).append(".5minute=").append(String.format("%2.2f",
          convertRate(meter.getFiveMinuteRate(), rateFactor))).append("|");
      sb.append(key).append(".15minute=").append(String.format("%2.2f",
          convertRate(meter.getFifteenMinuteRate(), rateFactor)));
      prefix = ",";
    }

    return sb.toString();
  }

  protected String counterGroupToString(String dateTime, String hostAndPort, double rateFactor,
                                        String groupName, Map<String, Counter> counters) {
    StringBuilder sb = new StringBuilder();
    sb.append(dateTime).append(" ");
    if(hostAndPort != null && !hostAndPort.isEmpty()) {
      sb.append(hostAndPort).append(" ");
    }
    sb.append("counter").append(" ");

    if(!groupName.isEmpty()) {
      sb.append(groupName).append(" ");
    }
    String prefix = "";
    for(Map.Entry<String, Counter> eachCounter: counters.entrySet()) {
      sb.append(prefix);
      sb.append(eachCounter.getKey()).append("=").append(eachCounter.getValue().getCount());
      prefix = ",";

    }
    return sb.toString();
  }

  protected String gaugeGroupToString(String dateTime, String hostAndPort, double rateFactor,
                                      String groupName, Map<String, Gauge> gauges) {
    StringBuilder sb = new StringBuilder();
    sb.append(dateTime).append(" ");
    if(hostAndPort != null && !hostAndPort.isEmpty()) {
      sb.append(hostAndPort).append(" ");
    }
    sb.append("guage").append(" ");

    if(!groupName.isEmpty()) {
      sb.append(groupName).append(" ");
    }
    String prefix = "";
    for(Map.Entry<String, Gauge> eachGauge: gauges.entrySet()) {
      sb.append(prefix).append(eachGauge.getKey()).append("=").append(eachGauge.getValue().getValue());
      prefix = ",";
    }
    return sb.toString();
  }

  protected String histogramGroupToString(String dateTime, String hostAndPort, double rateFactor,
                                          String groupName, Map<String, Histogram> histograms) {
    StringBuilder sb = new StringBuilder();
    sb.append(dateTime).append(" ");
    if(hostAndPort != null && !hostAndPort.isEmpty()) {
      sb.append(hostAndPort).append(" ");
    }
    sb.append("histogram").append(" ");

    if(!groupName.isEmpty()) {
      sb.append(groupName).append(" ");
    }

    String prefix = "";
    for(Map.Entry<String, Histogram> eachHistogram: histograms.entrySet()) {
      String key = eachHistogram.getKey();
      Histogram histogram = eachHistogram.getValue();
      sb.append(prefix);
      sb.append(key).append(".count=").append(histogram.getCount()).append("|");

      Snapshot snapshot = histogram.getSnapshot();

      sb.append(key).append(".min=").append(snapshot.getMin()).append("|");
      sb.append(key).append(".max=").append(snapshot.getMax()).append("|");
      sb.append(key).append(".mean=").append(String.format("%2.2f", snapshot.getMean())).append("|");
      sb.append(key).append(".stddev=").append(String.format("%2.2f", snapshot.getStdDev())).append("|");
      sb.append(key).append(".median=").append(String.format("%2.2f", snapshot.getMedian())).append("|");
      sb.append(key).append(".75%=").append(String.format("%2.2f", snapshot.get75thPercentile())).append("|");
      sb.append(key).append(".95%=").append(String.format("%2.2f", snapshot.get95thPercentile())).append("|");
      sb.append(key).append(".98%=").append(String.format("%2.2f", snapshot.get98thPercentile())).append("|");
      sb.append(key).append(".99%=").append(String.format("%2.2f", snapshot.get99thPercentile())).append("|");
      sb.append(key).append(".999%=").append(String.format("%2.2f", snapshot.get999thPercentile()));
      prefix = ",";
    }
    return sb.toString();
  }

  protected String timerGroupToString(String dateTime, String hostAndPort, double rateFactor,
                                 String groupName, Map<String, Timer> timers) {
    StringBuilder sb = new StringBuilder();

    sb.append(dateTime).append(" ");
    if(hostAndPort != null && !hostAndPort.isEmpty()) {
      sb.append(hostAndPort).append(" ");
    }
    sb.append("timer").append(" ");

    if(!groupName.isEmpty()) {
      sb.append(groupName).append(" ");
    }
    String prefix = "";
    for(Map.Entry<String, Timer> eachTimer: timers.entrySet()) {
      String key = eachTimer.getKey();
      Timer timer = eachTimer.getValue();
      Snapshot snapshot = timer.getSnapshot();

      sb.append(prefix);
      sb.append(key).append(".count=").append(timer.getCount()).append("|");
      sb.append(key).append(".meanrate=").append(String.format("%2.2f", convertRate(timer.getMeanRate(), rateFactor))).append("|");
      sb.append(key).append(".1minuterate=").append(String.format("%2.2f", convertRate(timer.getOneMinuteRate(), rateFactor))).append("|");
      sb.append(key).append(".5minuterate=").append(String.format("%2.2f", convertRate(timer.getFiveMinuteRate(), rateFactor))).append("|");
      sb.append(key).append(".15minuterate=").append(String.format("%2.2f", convertRate(timer.getFifteenMinuteRate(),rateFactor))).append("|");
      sb.append(key).append(".min=").append(String.format("%2.2f", convertRate(snapshot.getMin(), rateFactor))).append("|");
      sb.append(key).append(".max=").append(String.format("%2.2f", convertRate(snapshot.getMax(),rateFactor))).append("|");
      sb.append(key).append(".mean=").append(String.format("%2.2f", convertRate(snapshot.getMean(), rateFactor))).append("|");
      sb.append(key).append(".stddev=").append(String.format("%2.2f", convertRate(snapshot.getStdDev(),rateFactor))).append("|");
      sb.append(key).append(".median=").append(String.format("%2.2f", convertRate(snapshot.getMedian(), rateFactor))).append("|");
      sb.append(key).append(".75%=").append(String.format("%2.2f", convertRate(snapshot.get75thPercentile(), rateFactor))).append("|");
      sb.append(key).append(".95%=").append(String.format("%2.2f", convertRate(snapshot.get95thPercentile(), rateFactor))).append("|");
      sb.append(key).append(".98%=").append(String.format("%2.2f", convertRate(snapshot.get98thPercentile(), rateFactor))).append("|");
      sb.append(key).append(".99%=").append(String.format("%2.2f", convertRate(snapshot.get99thPercentile(), rateFactor))).append("|");
      sb.append(key).append(".999%=").append(String.format("%2.2f", convertRate(snapshot.get999thPercentile(),rateFactor)));
      prefix = ",";
    }

    return sb.toString();
  }

  protected double convertRate(double rate, double rateFactor) {
    return rate * rateFactor;
  }

}
