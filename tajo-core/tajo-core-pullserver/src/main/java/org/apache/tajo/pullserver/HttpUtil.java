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

package org.apache.tajo.pullserver;

import com.google.common.collect.Maps;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Map;

public class HttpUtil {
  public static Map<String,String> getParams(URI uri) throws UnsupportedEncodingException {
    return getParamsFromQuery(uri.getQuery());
  }

  /**
   * It parses a query string into key/value pairs
   *
   * @param queryString decoded query string
   * @return key/value pairs parsed from a given query string
   * @throws java.io.UnsupportedEncodingException
   */
  public static Map<String, String> getParamsFromQuery(String queryString) throws UnsupportedEncodingException {
    String [] queries = queryString.split("&");

    Map<String,String> params = Maps.newHashMap();
    String [] param;
    for (String q : queries) {
      param = q.split("=");
      params.put(param[0], param[1]);
    }

    return params;
  }

  public static String buildQuery(Map<String,String> params) throws UnsupportedEncodingException {
    StringBuilder sb = new StringBuilder();

    boolean first = true;
    for (Map.Entry<String,String> param : params.entrySet()) {
      if (!first) {
        sb.append("&");
      }
      sb.append(URLEncoder.encode(param.getKey(), "UTF-8")).
          append("=").
          append(URLEncoder.encode(param.getValue(), "UTF-8"));
      first = false;
    }

    return sb.toString();
  }
}
