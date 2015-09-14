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

package org.apache.tajo.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestUriUtil {
  static final String URI0 = "http://192.168.0.1/table1";
  static final String URI1 = "hbase:zk://192.168.0.1/table1";
  static final String URI2 = "jdbc:postgresql://192.168.0.1/table1";

  @Test
  public void testGetScheme() throws Exception {
    assertEquals("http", UriUtil.getScheme(URI0));
    assertEquals("hbase:zk", UriUtil.getScheme(URI1));
    assertEquals("jdbc:postgresql", UriUtil.getScheme(URI2));
  }

  @Test
  public void testAddParam() throws Exception {
    String userAdded = UriUtil.addParam(URI2, "user", "xxx");
    assertEquals("jdbc:postgresql://192.168.0.1/table1?user=xxx", userAdded);
    assertEquals("jdbc:postgresql://192.168.0.1/table1?user=xxx&pass=yyy", UriUtil.addParam(userAdded, "pass", "yyy"));
  }
}