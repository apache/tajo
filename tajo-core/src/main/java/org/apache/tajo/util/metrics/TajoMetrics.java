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

import com.codahale.metrics.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.util.metrics.reporter.TajoMetricsReporter;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TajoMetrics {
  private static final Log LOG = LogFactory.getLog(TajoMetrics.class);

  protected MetricRegistry metricRegistry;
  protected AtomicBoolean stop = new AtomicBoolean(false);
  protected String metricsGroupName;

  public TajoMetrics(String metricsGroupName) {
    this.metricsGroupName = metricsGroupName;
    this.metricRegistry = new MetricRegistry();
  }

  public void stop() {
    stop.set(true);
  }

  public MetricRegistry getRegistry() {
    return metricRegistry;
  }

  public void report(TajoMetricsReporter reporter) {
    try {
      reporter.report(metricRegistry.getGauges(),
          metricRegistry.getCounters(),
          metricRegistry.getHistograms(),
          metricRegistry.getMeters(),
          metricRegistry.getTimers());
    } catch (Exception e) {
      if(LOG.isDebugEnabled()) {
        LOG.warn("Metric report error:" + e.getMessage(), e);
      } else {
        LOG.warn("Metric report error:" + e.getMessage());
      }
    }
  }

  public Map<String, Metric> getMetrics() {
    return metricRegistry.getMetrics();
  }

  public SortedMap<String, Gauge> getGuageMetrics(MetricFilter filter) {
    if(filter == null) {
      filter = MetricFilter.ALL;
    }
    return metricRegistry.getGauges(filter);
  }

  public SortedMap<String, Counter> getCounterMetrics(MetricFilter filter) {
    if(filter == null) {
      filter = MetricFilter.ALL;
    }
    return metricRegistry.getCounters(filter);
  }

  public SortedMap<String, Histogram> getHistogramMetrics(MetricFilter filter) {
    if(filter == null) {
      filter = MetricFilter.ALL;
    }
    return metricRegistry.getHistograms(filter);
  }

  public SortedMap<String, Meter> getMeterMetrics(MetricFilter filter) {
    if(filter == null) {
      filter = MetricFilter.ALL;
    }
    return metricRegistry.getMeters(filter);
  }

  public SortedMap<String, Timer> getTimerMetrics(MetricFilter filter) {
    if(filter == null) {
      filter = MetricFilter.ALL;
    }
    return metricRegistry.getTimers(filter);
  }

  public void register(String contextName, MetricSet metricSet) {
    metricRegistry.register(MetricRegistry.name(metricsGroupName, contextName), metricSet);
  }

  public void register(String contextName, String itemName, Gauge gauge) {
    metricRegistry.register(makeMetricsName(metricsGroupName, contextName, itemName), gauge);
  }

  public Counter counter(String contextName, String itemName) {
    return metricRegistry.counter(makeMetricsName(metricsGroupName, contextName, itemName));
  }

  public Histogram histogram(String contextName, String itemName) {
    return metricRegistry.histogram(makeMetricsName(metricsGroupName, contextName, itemName));
  }

  public Meter meter(String contextName, String itemName) {
    return metricRegistry.meter(makeMetricsName(metricsGroupName, contextName, itemName));
  }

  public Timer timer(String contextName, String itemName) {
    return metricRegistry.timer(makeMetricsName(metricsGroupName, contextName, itemName));
  }

  public static String makeMetricsName(String metricsGroupName, String contextName, String itemName) {
    return MetricRegistry.name(metricsGroupName, contextName, itemName);
  }

}
