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

import com.codahale.metrics.Counter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.metrics.reporter.TajoMetricsScheduledReporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestSystemMetrics {
  Path testPropertyFile;
  Path metricsOutputFile;
  @Before
  public void setUp() throws Exception {
    testPropertyFile =
        new Path(CommonTestingUtil.getTestDir(), System.currentTimeMillis() + ".properties");

    metricsOutputFile =
        new Path(CommonTestingUtil.getTestDir(), System.currentTimeMillis() + ".out");

    FileOutputStream out = new FileOutputStream(testPropertyFile.toUri().getPath());
    out.write("reporter.null=org.apache.tajo.util.metrics.reporter.NullReporter\n".getBytes());
    out.write("reporter.file=org.apache.tajo.util.metrics.reporter.MetricsFileScheduledReporter\n".getBytes());
    out.write("reporter.console=org.apache.tajo.util.metrics.reporter.MetricsConsoleScheduledReporter\n".getBytes());

    out.write("test-file-group.reporters=file\n".getBytes());
    out.write("test-console-group.reporters=console\n".getBytes());
    out.write("test-find-console-group.reporters=console,file\n".getBytes());

    out.write(("test-file-group.file.filename=" + metricsOutputFile.toUri().getPath() + "\n").getBytes());
    out.write("test-file-group.file.period=5\n".getBytes());
  }

  @Test
  public void testMetricsReporter() throws Exception {
    TajoConf tajoConf = new TajoConf();
    tajoConf.set("tajo.metrics.property.file", testPropertyFile.toUri().getPath());
    TajoSystemMetrics tajoSystemMetrics = new TajoSystemMetrics(tajoConf, "test-file-group", "localhost");
    tajoSystemMetrics.start();

    Collection<TajoMetricsScheduledReporter> reporters = tajoSystemMetrics.getMetricsReporters();

    assertEquals(1, reporters.size());

    TajoMetricsScheduledReporter reporter = reporters.iterator().next();
    assertEquals(5, reporter.getPeriod());

    for(int i = 0; i < 10; i++) {
      tajoSystemMetrics.counter("test-group01", "test-item1").inc();
      tajoSystemMetrics.counter("test-group01", "test-item2").inc(2);
      tajoSystemMetrics.counter("test-group02", "test-item1").inc(3);
    }

    SortedMap<String, Counter> counterMap = tajoSystemMetrics.getRegistry().getCounters();
    Counter counter1 = counterMap.get("test-file-group.test-group01.test-item1");
    assertNotNull(counter1);
    assertEquals(10, counter1.getCount());

    Counter counter2 = counterMap.get("test-file-group.test-group01.test-item2");
    assertNotNull(counter2);
    assertEquals(20, counter2.getCount());

    //test findMetricsItemGroup method
    Map<String, Map<String, Counter>> groupItems = reporter.findMetricsItemGroup(counterMap);
    assertEquals(2, groupItems.size());

    Map<String, Counter> group01Items = groupItems.get("test-file-group.test-group01");
    assertEquals(2, group01Items.size());

    counter1 = group01Items.get("test-item1");
    assertNotNull(counter1);
    assertEquals(10, counter1.getCount());

    counter2 = group01Items.get("test-item2");
    assertNotNull(counter2);
    assertEquals(20, counter2.getCount());

    Map<String, Counter> group02Items = groupItems.get("test-file-group.test-group02");
    assertEquals(1, group02Items.size());

    reporter.report();

    BufferedReader reader = new BufferedReader(new InputStreamReader(
        new FileInputStream(metricsOutputFile.toUri().getPath())));

    String line = null;

    List<String> lines = new ArrayList<String>();
    while((line = reader.readLine()) != null) {
      lines.add(line);
    }

    assertEquals(2, lines.size());
  }

  @After
  public void tearDown() throws Exception {
    FileSystem fs = testPropertyFile.getFileSystem(new Configuration());
    fs.delete(testPropertyFile, false);
    fs.delete(metricsOutputFile, false);
  }
}
