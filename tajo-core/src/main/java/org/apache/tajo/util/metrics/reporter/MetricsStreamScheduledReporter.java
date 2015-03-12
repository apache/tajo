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
import com.codahale.metrics.Timer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class MetricsStreamScheduledReporter extends TajoMetricsScheduledReporter {
  private static final Log LOG = LogFactory.getLog(MetricsStreamScheduledReporter.class);

  protected OutputStream output;
  protected Locale locale;
  protected Clock clock;
  protected TimeZone timeZone;
  protected DateFormat dateFormat;

  private final byte[] NEW_LINE = "\n".getBytes();

  public MetricsStreamScheduledReporter() {
    dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    clock = Clock.defaultClock();
  }

  public void setOutput(OutputStream output) {
    this.output = output;
  }

  public void setLocale(Locale locale) {
    this.locale = locale;
  }

  public void setClock(Clock clock) {
    this.clock = clock;
  }

  public void setTimeZone(TimeZone timeZone) {
    this.dateFormat.setTimeZone(timeZone);
    this.timeZone = timeZone;
  }

  public void setDateFormat(DateFormat dateFormat) {
    this.dateFormat = dateFormat;
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges,
                     SortedMap<String, Counter> counters,
                     SortedMap<String, Histogram> histograms,
                     SortedMap<String, Meter> meters,
                     SortedMap<String, Timer> timers) {
    final String dateTime = dateFormat == null ? "" + clock.getTime() : dateFormat.format(new Date(clock.getTime()));

    if (!gauges.isEmpty()) {
      Map<String, Map<String, Gauge>> gaugeGroups = findMetricsItemGroup(gauges);

      for(Map.Entry<String, Map<String, Gauge>> eachGroup: gaugeGroups.entrySet()) {
        printGaugeGroup(dateTime, eachGroup.getKey(), eachGroup.getValue());
      }
    }

    if (!counters.isEmpty()) {
      Map<String, Map<String, Counter>> counterGroups = findMetricsItemGroup(counters);

      for(Map.Entry<String, Map<String, Counter>> eachGroup: counterGroups.entrySet()) {
        printCounterGroup(dateTime, eachGroup.getKey(), eachGroup.getValue());
      }
    }

    if (!histograms.isEmpty()) {
      Map<String, Map<String, Histogram>> histogramGroups = findMetricsItemGroup(histograms);

      for(Map.Entry<String, Map<String, Histogram>> eachGroup: histogramGroups.entrySet()) {
        printHistogramGroup(dateTime, eachGroup.getKey(), eachGroup.getValue());
      }
    }

    if (!meters.isEmpty()) {
      Map<String, Map<String, Meter>> meterGroups = findMetricsItemGroup(meters);

      for(Map.Entry<String, Map<String, Meter>> eachGroup: meterGroups.entrySet()) {
        printMeterGroup(dateTime, eachGroup.getKey(), eachGroup.getValue());
      }
    }

    if (!timers.isEmpty()) {
      Map<String, Map<String, Timer>> timerGroups = findMetricsItemGroup(timers);

      for(Map.Entry<String, Map<String, Timer>> eachGroup: timerGroups.entrySet()) {
        printTimerGroup(dateTime, eachGroup.getKey(), eachGroup.getValue());
      }
    }
    try {
      output.flush();
    } catch (IOException e) {
    }
  }

  private void printMeterGroup(String dateTime, String groupName, Map<String, Meter> meters) {
    try {
      output.write(meterGroupToString(dateTime, hostAndPort, rateFactor, groupName, meters).getBytes());
      output.write(NEW_LINE);
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }
  }

  private void printCounterGroup(String dateTime, String groupName, Map<String, Counter> counters) {
    try {
      output.write(counterGroupToString(dateTime, hostAndPort, rateFactor, groupName, counters).getBytes());
      output.write(NEW_LINE);
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }
  }

  private void printGaugeGroup(String dateTime, String groupName, Map<String, Gauge> gauges) {
    try {
      output.write(gaugeGroupToString(dateTime, hostAndPort, rateFactor, groupName, gauges).getBytes());
      output.write(NEW_LINE);
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }
  }

  private void printHistogramGroup(String dateTime, String groupName, Map<String, Histogram> histograms) {
    try {
      output.write(histogramGroupToString(dateTime, hostAndPort, rateFactor, groupName, histograms).getBytes());
      output.write(NEW_LINE);
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }
  }

  private void printTimerGroup(String dateTime, String groupName, Map<String, Timer> timers) {
    try {
      output.write(timerGroupToString(dateTime, hostAndPort, rateFactor, groupName, timers).getBytes());
      output.write(NEW_LINE);
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }
  }

  @Override
  public void close() {
    if(output != null) {
      try {
        output.close();
      } catch (IOException e) {
      }
    }

    super.close();
  }
}
