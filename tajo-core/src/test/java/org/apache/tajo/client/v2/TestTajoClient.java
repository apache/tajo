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

package org.apache.tajo.client.v2;

import net.jcip.annotations.NotThreadSafe;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.exception.UndefinedDatabaseException;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.junit.Assert.fail;

@NotThreadSafe
public class TestTajoClient extends QueryTestCaseBase {
  private static TajoClient clientv2;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = testingCluster.getConfiguration();

    clientv2 = new TajoClient(new ServiceDiscovery() {
      ServiceTracker tracker = ServiceTrackerFactory.get(conf);
      @Override
      public InetSocketAddress clientAddress() {
        return tracker.getClientServiceAddress();
      }
    });
  }

  @AfterClass
  public static void tearDown() throws Exception {
    clientv2.close();
  }

  @Test
  public void testExecuteUpdate() throws TajoException {
    clientv2.executeUpdate("create database tajoclientv2");
    clientv2.selectDB("tajoclientv2");
    clientv2.selectDB("default");
    clientv2.executeUpdate("drop database tajoclientv2");

    try {
      clientv2.selectDB("tajoclientv2");
      fail();
    } catch (UndefinedDatabaseException e) {
    }
  }
}
