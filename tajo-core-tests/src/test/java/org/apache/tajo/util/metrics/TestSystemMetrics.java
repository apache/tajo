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
import org.apache.tajo.metrics.Master;
import org.apache.tajo.metrics.MetricsUtil;
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

    out.write("MASTER-JVM.reporters=console\n".getBytes());
    out.write("MASTER.reporters=file\n".getBytes());
    out.write("test-console-group.reporters=console\n".getBytes());
    out.write("test-find-console-group.reporters=console,file\n".getBytes());

    out.write(("MASTER.file.filename=" + metricsOutputFile.toUri().getPath() + "\n").getBytes());
    out.write("MASTER.file.period=5\n".getBytes());
    out.close();
  }

  @Test
  public void testMetricsReporter() throws Exception {
    TajoConf tajoConf = new TajoConf();
    tajoConf.set("tajo.metrics.property.file", testPropertyFile.toUri().getPath());
    TajoSystemMetrics tajoSystemMetrics = new TajoSystemMetrics(tajoConf, org.apache.tajo.metrics.Master.class,
        "localhost");
    tajoSystemMetrics.start();

    Collection<TajoMetricsScheduledReporter> reporters = tajoSystemMetrics.getMetricsReporters();

    assertEquals(2, reporters.size());

    TajoMetricsScheduledReporter reporter = reporters.iterator().next();
    assertEquals(5, reporter.getPeriod());

    for(int i = 0; i < 10; i++) {
      tajoSystemMetrics.counter(Master.Query.FAILED).inc();
      tajoSystemMetrics.counter(Master.Query.COMPLETED).inc(2);
      tajoSystemMetrics.counter(Master.Cluster.ACTIVE_NODES).inc(3);
    }

    SortedMap<String, Counter> counterMap = tajoSystemMetrics.getRegistry().getCounters();
    Counter counter1 = counterMap.get("MASTER.QUERY.FAILED");
    assertNotNull(counter1);
    assertEquals(10, counter1.getCount());

    Counter counter2 = counterMap.get("MASTER.QUERY.COMPLETED");
    assertNotNull(counter2);
    assertEquals(20, counter2.getCount());

    Counter counter3 = counterMap.get("MASTER.CLUSTER.ACTIVE_NODES");
    assertNotNull(counter3);
    assertEquals(30, counter3.getCount());

    //test findMetricsItemGroup method
    Map<String, Map<String, Counter>> groupItems = reporter.findMetricsItemGroup(counterMap);
    assertEquals(2, groupItems.size());

    Map<String, Counter> group01Items = groupItems.get(MetricsUtil.getCanonicalContextName(Master.Query.class));
    assertEquals(2, group01Items.size());

    counter1 = group01Items.get(Master.Query.FAILED.name());
    assertNotNull(counter1);
    assertEquals(10, counter1.getCount());

    counter2 = group01Items.get(Master.Query.COMPLETED.name());
    assertNotNull(counter2);
    assertEquals(20, counter2.getCount());

    Map<String, Counter> group02Items = groupItems.get(MetricsUtil.getCanonicalContextName(Master.Cluster.class));
    assertEquals(1, group02Items.size());

    reporter.report();

    BufferedReader reader = new BufferedReader(new InputStreamReader(
        new FileInputStream(metricsOutputFile.toUri().getPath())));

    String line;

    List<String> lines = new ArrayList<>();
    while((line = reader.readLine()) != null) {
      lines.add(line);
    }

    assertEquals(2, lines.size());
    tajoSystemMetrics.stop();
  }

  @After
  public void tearDown() throws Exception {
    FileSystem fs = testPropertyFile.getFileSystem(new Configuration());
    fs.delete(testPropertyFile, false);
    fs.delete(metricsOutputFile, false);
  }
}
