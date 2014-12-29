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

public class PlatformUtil {
  public static enum OsType {
    WINDOWS,
    MAC,
    LINUX_OR_UNIX,
    SOLARIS,
    UNKNOWN
  }

  public static String getOsName() {
    return System.getProperty("os.name").toLowerCase();
  }

  public static OsType getOsType() {
    String osName = System.getProperty("os.name").toLowerCase();

    if (PlatformUtil.isWindows(osName)) {
      return OsType.WINDOWS;
    } else if (isMac(osName)) {
      return OsType.MAC;
    } else if (isUnix(osName)) {
      return OsType.LINUX_OR_UNIX;
    } else if (isSolaris(osName)) {
      return OsType.SOLARIS;
    } else {
      throw new RuntimeException("Unknown OS Type: " + osName);
    }
  }

  private static boolean isWindows(String osName) {
    return (osName.indexOf("win") >= 0);
  }

  private static boolean isMac(String osName) {
    return (osName.indexOf("mac") >= 0);
  }

  private static boolean isUnix(String osName) {
    return (osName.indexOf("nix") >= 0 || osName.indexOf("nux") >= 0 || osName.indexOf("aix") > 0 );
  }

  private static boolean isSolaris(String osName) {
    return (osName.indexOf("sunos") >= 0) || (osName.indexOf("solaris") >= 0);
  }
}
