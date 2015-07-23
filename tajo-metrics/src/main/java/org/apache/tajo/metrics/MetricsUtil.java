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

package org.apache.tajo.metrics;

import java.util.regex.Pattern;

/**
 * Each metric name can be divided into group name, context name, and item name.
 */
public class MetricsUtil {
  public static final String DELIMITER = ".";

  static final Pattern pattern;

  static {
    pattern = Pattern.compile("\\.");
  }

  private static void checkMetricsClass(Class clazz) {
    if (!clazz.getName().substring(0, 23).equals("org.apache.tajo.metrics")) {
      throw new RuntimeException("Invalid Metrics Class: " + clazz.getName());
    }
  }

  public static String getGroupName(Class clazz) {
    checkMetricsClass(clazz);


    String [] names = pattern.split(clazz.getCanonicalName().substring(24));
    if (names.length > 0) {
      return names[0].toUpperCase();
    } else {
      if (clazz.isEnum()) {
        return clazz.getSimpleName().toUpperCase();
      } else {
        throw new RuntimeException("Invalid Metrics Class: " + clazz.getName());
      }
    }
  }

  public static String getGroupName(Enum<?> e) {
    checkMetricsClass(e.getClass());

    return pattern.split(e.getClass().getCanonicalName().substring(24))[0].toUpperCase();
  }

  public static String getCanonicalContextName(Class<? extends Enum<?>> clazz) {
    checkMetricsClass(clazz);

    if (!clazz.isEnum()) {
      throw new RuntimeException("Invalid Context Enum: " + clazz.getName());
    }

    return getGroupName(clazz) + DELIMITER + clazz.getSimpleName().toUpperCase();
  }

  public static String getContextName(Class<? extends Enum<?>> clazz) {
    checkMetricsClass(clazz);

    if (!clazz.isEnum()) {
      throw new RuntimeException("Invalid Context Enum: " + clazz.getName());
    }

    return clazz.getSimpleName().toUpperCase();
  }

  public static String getCanonicalName(Enum<?> e) {
    checkMetricsClass(e.getClass());

    String [] groupAndContextNames = pattern.split(e.getClass().getCanonicalName().substring(24));
    if (groupAndContextNames.length != 2) {
      throw new RuntimeException("Invalid Metric Enum Type: " + e.getClass().getName());
    }
    return (groupAndContextNames[0] + DELIMITER + groupAndContextNames[1] + DELIMITER + e.name()).toUpperCase();
  }

  public static String getSimpleName(Enum<?> e) {
    return e.name();
  }
}
