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
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.metrics.MetricsUtil;
import org.apache.tajo.util.metrics.reporter.TajoMetricsReporter;

import java.util.Map;
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

  public void register(Enum<?> item, Gauge gauge) {
    Preconditions.checkArgument(metricsGroupName.equals(MetricsUtil.getGroupName(item)));
    metricRegistry.register(MetricsUtil.getCanonicalName(item), gauge);
  }

  public void register(Class<? extends Enum<?>> context, MetricSet metricSet) {
    Preconditions.checkArgument(metricsGroupName.equals(MetricsUtil.getGroupName(context)));

    metricRegistry.register(MetricsUtil.getCanonicalContextName(context), metricSet);
  }

  public Counter counter(Enum<?> item) {
    return metricRegistry.counter(MetricsUtil.getCanonicalName(item));
  }

  public Histogram histogram(Enum<?> item) {
    return metricRegistry.histogram(MetricsUtil.getCanonicalName(item));
  }

  public Meter meter(Enum<?> item) {
    return metricRegistry.meter(MetricsUtil.getCanonicalName(item));
  }

  public Timer timer(Enum<?> item) {
    return metricRegistry.timer(MetricsUtil.getCanonicalName(item));
  }
}
