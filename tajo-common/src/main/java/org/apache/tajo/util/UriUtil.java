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

import java.net.URI;

/**
 * Utility for URI representation
 */
public class UriUtil {
  public static String getScheme(URI uri) {
    return getScheme(uri.toASCIIString());
  }

  public static String getScheme(String uri) {
    return uri.substring(0, uri.indexOf(":/"));
  }

  /**
   * Add an query parameter to an existing URI.
   * @param uri   an URI
   * @param name  Parameter name
   * @param value Parameter value
   * @return An URI including the given parameter
   */
  public static String addParam(String uri, String name, String value) {
    final String questionMarkOrAnd = uri.split("\\?").length > 1 ? "&" : "?";
    return uri + questionMarkOrAnd + name + "=" + value;
  }
}
