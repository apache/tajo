/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.tajo.storage.StorageConstants;
import java.util.Map;

public class TimeZoneUtil {
  static ImmutableMap<String, String> timeZoneIdMap;

  static {
    timeZoneIdMap = load();
  }

  private static ImmutableMap<String, String> load() {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder();
    String[] timezoneIds = java.util.TimeZone.getAvailableIDs();
    for (String timezoneId : timezoneIds) {
      builder.put(timezoneId.toUpperCase(), timezoneId);
    }

    return builder.build();
  }

  private static String find(String timezoneId) {
    if (timezoneId == null) {
      return null;
    }

    return timeZoneIdMap.get(timezoneId.toUpperCase());
  }

  public static String getValidTimezone(String timeZoneId) {
    String convertedTimezone = find(timeZoneId);
    if (convertedTimezone != null) {
      return convertedTimezone;
    }

    return timeZoneId;
  }

  public static boolean isTimezone(String key) {
    return StorageConstants.TIMEZONE.equalsIgnoreCase(key);
  }
}
