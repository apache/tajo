/*
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

package org.apache.tajo.storage.jdbc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestConnectionInfo {
  @Test
  public final void testGetConnectionInfoType1() {
    ConnectionInfo c1 = ConnectionInfo.fromURI("jdbc:mysql://localhost:55840?user=testuser&password=testpass");
    assertEquals("jdbc:mysql", c1.scheme);
    assertEquals("localhost", c1.host);
    assertEquals("testuser", c1.user);
    assertEquals("testpass", c1.password);
    assertNull(c1.dbName);
    assertNull(c1.tableName);
    assertEquals(0, c1.params.size());
  }

  @Test
  public final void testGetConnectionInfoType2() {
    ConnectionInfo c1 = ConnectionInfo.fromURI(
        "jdbc:mysql://localhost:55840/db1?table=tb1&user=testuser&password=testpass&TZ=GMT+9");
    assertEquals("jdbc:mysql", c1.scheme);
    assertEquals("localhost", c1.host);
    assertEquals("testuser", c1.user);
    assertEquals("testpass", c1.password);
    assertEquals("db1", c1.dbName);
    assertEquals("tb1", c1.tableName);
    assertEquals(1, c1.params.size());
    assertEquals("GMT+9", c1.params.get("TZ"));
  }
}