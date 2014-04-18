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

package org.apache.tajo.worker.dataserver;

import com.google.common.collect.Maps;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestHttpUtil {
  private URI uri = URI.create("http://127.0.0.1:80/?key1=val1&key2=val2");

  @Test
  public void testGetParams() throws UnsupportedEncodingException {
    Map<String,String> params = HttpUtil.getParamsFromQuery(uri.getQuery());
    assertEquals(2, params.size());
    assertEquals("val1", params.get("key1"));
    assertEquals("val2", params.get("key2"));
  }

  @Test
  public void testBuildQuery() throws UnsupportedEncodingException {
    Map<String,String> params = Maps.newTreeMap();
    params.put("key1", "val1");
    params.put("key2", "val2");
    String query = HttpUtil.buildQuery(params);
    assertEquals(uri.getQuery(), query);
  }
}
