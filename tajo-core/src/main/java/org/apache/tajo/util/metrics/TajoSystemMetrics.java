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

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.event.ConfigurationEvent;
import org.apache.commons.configuration.event.ConfigurationListener;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.metrics.MetricsUtil;
import org.apache.tajo.util.metrics.reporter.TajoMetricsScheduledReporter;

import java.util.*;

public class TajoSystemMetrics extends TajoMetrics {
  private static final Log LOG = LogFactory.getLog(TajoSystemMetrics.class);

  private PropertiesConfiguration metricsProps;

  private Thread propertyChangeChecker;

  private String hostAndPort;

  private List<TajoMetricsScheduledReporter> metricsReporters = new ArrayList<>();

  private boolean inited = false;

  private String metricsPropertyFileName;

  private JmxReporter jmxReporter;

  public TajoSystemMetrics(TajoConf tajoConf, Class clazz, String hostAndPort) {
    super(MetricsUtil.getGroupName(clazz));

    this.hostAndPort = hostAndPort;
    try {
      this.metricsPropertyFileName = tajoConf.getVar(TajoConf.ConfVars.METRICS_PROPERTY_FILENAME);
      this.metricsProps = new PropertiesConfiguration(metricsPropertyFileName);
      this.metricsProps.addConfigurationListener(new MetricsReloadListener());
      FileChangedReloadingStrategy reloadingStrategy = new FileChangedReloadingStrategy();
      reloadingStrategy.setRefreshDelay(5 * 1000);
      this.metricsProps.setReloadingStrategy(reloadingStrategy);
    } catch (ConfigurationException e) {
      LOG.warn(e.getMessage(), e);
    }

    // PropertiesConfiguration fire configurationChanged after getXXX()
    // So neeaded calling getXXX periodically
    propertyChangeChecker = new Thread() {
      public void run() {
        while(!stop.get()) {
          String value = metricsProps.getString("reporter.file");
          try {
            Thread.sleep(10 * 1000);
          } catch (InterruptedException e) {
          }
        }
      }
    };

    propertyChangeChecker.start();
  }

  public Collection<TajoMetricsScheduledReporter> getMetricsReporters() {
    synchronized (metricsReporters) {
      return Collections.unmodifiableCollection(metricsReporters);
    }
  }

  @Override
  public void stop() {
    super.stop();
    if(propertyChangeChecker != null) {
      propertyChangeChecker.interrupt();
    }
    stopAndClearReporter();
    jmxReporter.close();
  }

  protected void stopAndClearReporter() {
    synchronized(metricsReporters) {
      metricsReporters.forEach(TajoMetricsScheduledReporter::close);

      metricsReporters.clear();
    }
  }

  public void start() {
    setMetricsReporter(metricsGroupName);

    final String jvmMetricsName = metricsGroupName + "-JVM";
    setMetricsReporter(jvmMetricsName);

    if(!inited) {
      metricRegistry.register(MetricRegistry.name(jvmMetricsName, "MEMORY"), new MemoryUsageGaugeSet());
      metricRegistry.register(MetricRegistry.name(jvmMetricsName, "FILE"), new FileDescriptorRatioGauge());
      metricRegistry.register(MetricRegistry.name(jvmMetricsName, "GC"), new GarbageCollectorMetricSet());
      metricRegistry.register(MetricRegistry.name(jvmMetricsName, "THREAD"), new ThreadStatesGaugeSet());
      metricRegistry.register(MetricRegistry.name(jvmMetricsName, "LOG"), new LogEventGaugeSet());
      jmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain("Tajo")
              .createsObjectNamesWith(new TajoJMXObjectNameFactory()).build();
      jmxReporter.start();
    }
    inited = true;
  }

  private void setMetricsReporter(String groupName) {
    // reporter name -> class name
    Map<String, String> reporters = new HashMap<>();

    List<String> reporterNames = metricsProps.getList(groupName + ".reporters");
    if(reporterNames.isEmpty()) {
      LOG.warn("No property " + groupName + ".reporters in " + metricsPropertyFileName);
      return;
    }

    Map<String, String> allReporterProperties = new HashMap<>();

    Iterator<String> keys = metricsProps.getKeys();
    while (keys.hasNext()) {
      String key = keys.next();
      String value = metricsProps.getString(key);
      if(key.indexOf("reporter.") == 0) {
        String[] tokens = key.split("\\.");
        if(tokens.length == 2) {
          reporters.put(tokens[1], value);
        }
      } else if(key.indexOf(groupName + ".") == 0) {
        String[] tokens = key.split("\\.");
        if(tokens.length > 2) {
          allReporterProperties.put(key, value);
        }
      }
    }

    synchronized(metricsReporters) {
      for(String eachReporterName: reporterNames) {
        if("null".equals(eachReporterName)) {
          continue;
        }
        String reporterClass = reporters.get(eachReporterName);
        if(reporterClass == null) {
          LOG.warn("No metrics reporter definition[" + eachReporterName + "] in " + metricsPropertyFileName);
          continue;
        }

        Map<String, String> eachMetricsReporterProperties = findMetircsProperties(allReporterProperties,
            groupName + "." + eachReporterName);

        try {
          Object reporterObject = Class.forName(reporterClass).newInstance();
          if(!(reporterObject instanceof TajoMetricsScheduledReporter)) {
            LOG.warn(reporterClass + " is not subclass of " + TajoMetricsScheduledReporter.class.getCanonicalName());
            continue;
          }
          TajoMetricsScheduledReporter reporter = (TajoMetricsScheduledReporter)reporterObject;
          reporter.init(metricRegistry, groupName, hostAndPort, eachMetricsReporterProperties);
          reporter.start();

          metricsReporters.add(reporter);
          LOG.info("Started metrics reporter " + reporter.getClass().getCanonicalName() + " for " + groupName);
        } catch (ClassNotFoundException e) {
          LOG.warn("No metrics reporter class[" + eachReporterName + "], required class= " + reporterClass);
          continue;
        } catch (Exception e) {
          LOG.warn("Can't initiate metrics reporter class[" + eachReporterName + "]" + e.getMessage() , e);
          continue;
        }
      }
    }
  }

  private Map<String, String> findMetircsProperties(Map<String, String> allReporterProperties, String findKey) {
    Map<String, String> metricsProperties = new HashMap<>();

    for (Map.Entry<String, String> entry: allReporterProperties.entrySet()) {
      String eachKey = entry.getKey();
      if (eachKey.indexOf(findKey) == 0) {
        metricsProperties.put(eachKey, entry.getValue());
      }
    }
    return metricsProperties;
  }

  class MetricsReloadListener implements ConfigurationListener {
    @Override
    public synchronized void configurationChanged(ConfigurationEvent event) {
      if (!event.isBeforeUpdate()) {
        stopAndClearReporter();
        start();
      }
    }
  }
}
