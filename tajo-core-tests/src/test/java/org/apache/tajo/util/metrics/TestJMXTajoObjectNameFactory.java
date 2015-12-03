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

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class TestJMXTajoObjectNameFactory {

  TajoJMXObjectNameFactory jmxObjectNameFactory;

  @Before
  public void setUp() {
    jmxObjectNameFactory = new TajoJMXObjectNameFactory();
  }

  @Test
  public void testCreateNameWithSingleDepth() throws MalformedObjectNameException {
    ObjectName objectName = jmxObjectNameFactory.createName("timers", "Tajo", "ACTIVE_NODES");
    ObjectName expectedObjectName = new ObjectName("Tajo:name=ACTIVE_NODES");
    assertEquals(objectName, expectedObjectName);
  }

  @Test
  public void testCreateNameWith2Depth() throws MalformedObjectNameException {
    ObjectName objectName = jmxObjectNameFactory.createName("timers", "Tajo", "MASTER-JVM.File");
    ObjectName expectedObjectName = new ObjectName("Tajo:type=MASTER-JVM,name=File");
    assertEquals(objectName, expectedObjectName);
  }

  @Test
  public void testCreateNameWith3Depth() throws MalformedObjectNameException {
    ObjectName objectName = jmxObjectNameFactory.createName("timers", "Tajo", "MASTER.CLUSTER.ACTIVE_NODES");
    ObjectName expectedObjectName = new ObjectName("Tajo:type=MASTER,context=CLUSTER,name=ACTIVE_NODES");
    assertEquals(objectName, expectedObjectName);
  }

  @Test
  public void testCreateNameWith4Depth() throws MalformedObjectNameException {
    ObjectName objectName = jmxObjectNameFactory.createName("timers", "Tajo", "MASTER-JVM.MEMORY.heap.used");
    ObjectName expectedObjectName = new ObjectName("Tajo:type=MASTER-JVM,context=MEMORY,name=heap.used");
    assertEquals(objectName, expectedObjectName);
  }
}
