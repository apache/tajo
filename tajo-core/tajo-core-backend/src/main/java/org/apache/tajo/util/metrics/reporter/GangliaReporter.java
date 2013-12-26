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
import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetricSlope;
import info.ganglia.gmetric4j.gmetric.GMetricType;
import info.ganglia.gmetric4j.gmetric.GangliaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.SortedMap;

import static com.codahale.metrics.MetricRegistry.name;

public class GangliaReporter extends TajoMetricsScheduledReporter {
  private static final Logger LOG = LoggerFactory.getLogger(GangliaReporter.class);
  public static final String REPORTER_NAME = "ganglia";

  private GMetric ganglia;
  private String prefix;
  private int tMax = 60;
  private int dMax = 0;

  @Override
  protected String getReporterName() {
    return REPORTER_NAME;
  }

  @Override
  protected void afterInit() {
    String server = metricsProperties.get(metricsPropertyKey + "server");
    String port = metricsProperties.get(metricsPropertyKey + "port");

    if(server == null || server.isEmpty()) {
      LOG.warn("No " + metricsPropertyKey + "server property in tajo-metrics.properties");
      return;
    }

    if(port == null || port.isEmpty()) {
      LOG.warn("No " + metricsPropertyKey + "port property in tajo-metrics.properties");
      return;
    }

    try {
      ganglia = new GMetric(server, Integer.parseInt(port), GMetric.UDPAddressingMode.MULTICAST, 1);
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public void settMax(int tMax) {
    this.tMax = tMax;
  }

  public void setdMax(int dMax) {
    this.dMax = dMax;
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges,
                     SortedMap<String, Counter> counters,
                     SortedMap<String, Histogram> histograms,
                     SortedMap<String, Meter> meters,
                     SortedMap<String, Timer> timers) {
    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
      reportGauge(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      reportCounter(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
      reportHistogram(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      reportMeter(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Timer> entry : timers.entrySet()) {
      reportTimer(entry.getKey(), entry.getValue());
    }
  }

  private void reportTimer(String name, Timer timer) {
    final String group = group(name);
    try {
      final Snapshot snapshot = timer.getSnapshot();

      announce(prefix(name, "max"), group, convertDuration(snapshot.getMax()), getDurationUnit());
      announce(prefix(name, "mean"), group, convertDuration(snapshot.getMean()), getDurationUnit());
      announce(prefix(name, "min"), group, convertDuration(snapshot.getMin()), getDurationUnit());
      announce(prefix(name, "stddev"), group, convertDuration(snapshot.getStdDev()), getDurationUnit());

      announce(prefix(name, "p50"), group, convertDuration(snapshot.getMedian()), getDurationUnit());
      announce(prefix(name, "p75"),
          group,
          convertDuration(snapshot.get75thPercentile()),
          getDurationUnit());
      announce(prefix(name, "p95"),
          group,
          convertDuration(snapshot.get95thPercentile()),
          getDurationUnit());
      announce(prefix(name, "p98"),
          group,
          convertDuration(snapshot.get98thPercentile()),
          getDurationUnit());
      announce(prefix(name, "p99"),
          group,
          convertDuration(snapshot.get99thPercentile()),
          getDurationUnit());
      announce(prefix(name, "p999"),
          group,
          convertDuration(snapshot.get999thPercentile()),
          getDurationUnit());

      reportMetered(name, timer, group, "calls");
    } catch (GangliaException e) {
      LOG.warn("Unable to report timer {}", name, e);
    }
  }

  private void reportMeter(String name, Meter meter) {
    final String group = group(name);
    try {
      reportMetered(name, meter, group, "events");
    } catch (GangliaException e) {
      LOG.warn("Unable to report meter {}", name, e);
    }
  }

  private void reportMetered(String name, Metered meter, String group, String eventName) throws GangliaException {
    final String unit = eventName + '/' + getRateUnit();
    announce(prefix(name, "count"), group, meter.getCount(), eventName);
    announce(prefix(name, "m1_rate"), group, convertRate(meter.getOneMinuteRate()), unit);
    announce(prefix(name, "m5_rate"), group, convertRate(meter.getFiveMinuteRate()), unit);
    announce(prefix(name, "m15_rate"), group, convertRate(meter.getFifteenMinuteRate()), unit);
    announce(prefix(name, "mean_rate"), group, convertRate(meter.getMeanRate()), unit);
  }

  private void reportHistogram(String name, Histogram histogram) {
    final String group = group(name);
    try {
      final Snapshot snapshot = histogram.getSnapshot();

      announce(prefix(name, "count"), group, histogram.getCount(), "");
      announce(prefix(name, "max"), group, snapshot.getMax(), "");
      announce(prefix(name, "mean"), group, snapshot.getMean(), "");
      announce(prefix(name, "min"), group, snapshot.getMin(), "");
      announce(prefix(name, "stddev"), group, snapshot.getStdDev(), "");
      announce(prefix(name, "p50"), group, snapshot.getMedian(), "");
      announce(prefix(name, "p75"), group, snapshot.get75thPercentile(), "");
      announce(prefix(name, "p95"), group, snapshot.get95thPercentile(), "");
      announce(prefix(name, "p98"), group, snapshot.get98thPercentile(), "");
      announce(prefix(name, "p99"), group, snapshot.get99thPercentile(), "");
      announce(prefix(name, "p999"), group, snapshot.get999thPercentile(), "");
    } catch (GangliaException e) {
      LOG.warn("Unable to report histogram {}", name, e);
    }
  }

  private void reportCounter(String name, Counter counter) {
    final String group = group(name);
    try {
      announce(prefix(name, "count"), group, counter.getCount(), "");
    } catch (GangliaException e) {
      LOG.warn("Unable to report counter {}", name, e);
    }
  }

  private void reportGauge(String name, Gauge gauge) {
    final String group = group(name);
    final Object obj = gauge.getValue();

    try {
      ganglia.announce(name(prefix, name), String.valueOf(obj), detectType(obj), "",
          GMetricSlope.BOTH, tMax, dMax, group);
    } catch (GangliaException e) {
      LOG.warn("Unable to report gauge {}", name, e);
    }
  }

  private void announce(String name, String group, double value, String units) throws GangliaException {
    ganglia.announce(name,
        Double.toString(value),
        GMetricType.DOUBLE,
        units,
        GMetricSlope.BOTH,
        tMax,
        dMax,
        group);
  }

  private void announce(String name, String group, long value, String units) throws GangliaException {
    final String v = Long.toString(value);
    ganglia.announce(name,
        v,
        GMetricType.DOUBLE,
        units,
        GMetricSlope.BOTH,
        tMax,
        dMax,
        group);
  }

  private GMetricType detectType(Object o) {
    if (o instanceof Float) {
      return GMetricType.FLOAT;
    } else if (o instanceof Double) {
      return GMetricType.DOUBLE;
    } else if (o instanceof Byte) {
      return GMetricType.INT8;
    } else if (o instanceof Short) {
      return GMetricType.INT16;
    } else if (o instanceof Integer) {
      return GMetricType.INT32;
    } else if (o instanceof Long) {
      return GMetricType.DOUBLE;
    }
    return GMetricType.STRING;
  }

  private String group(String name) {
    String[] tokens = name.split("\\.");
    if(tokens.length < 3) {
      return "";
    }
    return tokens[0] + "." + tokens[1];
  }

  private String prefix(String name, String n) {
    return name(prefix, name, n);
  }
}
