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

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.util.metrics.GroupNameMetricsFilter;
import org.apache.tajo.util.metrics.MetricsFilterList;
import org.apache.tajo.util.metrics.RegexpMetricsFilter;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class TajoMetricsScheduledReporter extends TajoMetricsReporter implements Closeable {
  private static final Log LOG = LogFactory.getLog(TajoMetricsScheduledReporter.class);

  public static final String PERIOD_KEY = "period";

  protected MetricRegistry registry;
  protected ScheduledExecutorService executor;
  protected MetricFilter filter;
  protected double durationFactor;
  protected String durationUnit;
  protected double rateFactor;
  protected String rateUnit;
  protected Map<String, String> metricsProperties;
  protected String metricsName;
  protected String metricsPropertyKey;
  protected String hostAndPort;
  protected long period;

  protected abstract String getReporterName();
  protected abstract void afterInit();

  private static class NamedThreadFactory implements ThreadFactory {
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    private NamedThreadFactory(String name) {
      final SecurityManager s = System.getSecurityManager();
      this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
      this.namePrefix = "metrics-" + name + "-thread-";
    }

    @Override
    public Thread newThread(Runnable r) {
      final Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
      t.setDaemon(true);
      if (t.getPriority() != Thread.NORM_PRIORITY) {
        t.setPriority(Thread.NORM_PRIORITY);
      }
      return t;
    }
  }

  public long getPeriod() {
    return period;
  }

  public void init(MetricRegistry registry,
                   String metricsName,
                   String hostAndPort,
                   Map<String, String> metricsProperties) {
    this.registry = registry;
    this.metricsName = metricsName;
    this.executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(metricsName));
    this.rateFactor = TimeUnit.SECONDS.toSeconds(1);
    this.rateUnit = calculateRateUnit(TimeUnit.MILLISECONDS);
    this.durationFactor = 1.0 / TimeUnit.MILLISECONDS.toNanos(1);
    this.durationUnit = TimeUnit.MILLISECONDS.toString().toLowerCase(Locale.US);
    this.metricsProperties = metricsProperties;
    this.metricsPropertyKey = metricsName + "." + getReporterName() + ".";
    this.hostAndPort = hostAndPort;

    MetricsFilterList filterList = new MetricsFilterList();
    filterList.addMetricFilter(new GroupNameMetricsFilter(metricsName));

    String regexpFilterKey = metricsPropertyKey + "regexp.";
    Set<String> regexpExpressions = new HashSet<String>();

    for(Map.Entry<String, String> entry: metricsProperties.entrySet()) {
      String key = entry.getKey();
      if(key.indexOf(regexpFilterKey) == 0) {
        regexpExpressions.add(entry.getValue());
      }
    }

    if(!regexpExpressions.isEmpty()) {
      filterList.addMetricFilter(new RegexpMetricsFilter(regexpExpressions));
    }
    this.filter = filterList;

    this.period = 60;
    if(metricsProperties.get(metricsPropertyKey + PERIOD_KEY) != null) {
      this.period = Integer.parseInt(metricsProperties.get(metricsPropertyKey + PERIOD_KEY));
    }
    afterInit();
  }

  public void start() {
    start(period, TimeUnit.SECONDS);
  }

  /**
   * Starts the reporter polling at the given period.
   *
   * @param period the amount of time between polls
   * @param unit   the unit for {@code period}
   */
  public void start(long period, TimeUnit unit) {
    this.period = period;
    executor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          report();
        } catch (Exception e) {
          if(LOG.isDebugEnabled()) {
            LOG.warn("Metric report error:" + e.getMessage(), e);
          } else {
            LOG.warn("Metric report error:" + e.getMessage(), e);
          }
        }
      }
    }, period, period, unit);
  }

  /**
   * Stops the reporter and shuts down its thread of execution.
   */
  public void stop() {
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException ignored) {
      // do nothing
    }
  }

  /**
   * Stops the reporter and shuts down its thread of execution.
   */
  @Override
  public void close() {
    stop();
  }

  /**
   * Report the current values of all metrics in the registry.
   */
  public void report() {
    report(registry.getGauges(filter),
        registry.getCounters(filter),
        registry.getHistograms(filter),
        registry.getMeters(filter),
        registry.getTimers(filter));
  }

  protected String getRateUnit() {
    return rateUnit;
  }

  protected String getDurationUnit() {
    return durationUnit;
  }

  protected double convertDuration(double duration) {
    return duration * durationFactor;
  }

  protected double convertRate(double rate) {
    return rate * rateFactor;
  }

  private String calculateRateUnit(TimeUnit unit) {
    final String s = unit.toString().toLowerCase(Locale.US);
    return s.substring(0, s.length() - 1);
  }
}
