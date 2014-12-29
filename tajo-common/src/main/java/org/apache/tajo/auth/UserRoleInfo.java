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

package org.apache.tajo.auth;

import org.apache.tajo.util.PlatformUtil;

import java.lang.reflect.Method;

public class UserRoleInfo {
  private final String username;

  public UserRoleInfo(String username) {
    this.username = username;
  }

  public String getUserName() {
    return username;
  }

  @Override
  public String toString() {
    return "user=" + username;
  }

  public synchronized static UserRoleInfo getCurrentUser() {
    Class<?> c;
    Object   o = null;
    Method method = null;
    String userName;

    PlatformUtil.OsType osType = PlatformUtil.getOsType();

    try {
      switch (osType) {
      case WINDOWS:
        c = Class.forName("com.sun.security.auth.module.NTSystem");
        o = Class.forName("com.sun.security.auth.module.NTSystem").newInstance();
        method = c.getDeclaredMethod("getName");
        break;
      case LINUX_OR_UNIX:
      case MAC:
        c = Class.forName("com.sun.security.auth.module.UnixSystem");
        o = Class.forName("com.sun.security.auth.module.UnixSystem").newInstance();
        method = c.getDeclaredMethod("getUsername");
        break;
      case SOLARIS:
        c = Class.forName("com.sun.security.auth.module.SolarisSystem");
        o = Class.forName("com.sun.security.auth.module.SolarisSystem").newInstance();
        method = c.getDeclaredMethod("getUsername");
        break;

      default:
        throw new IllegalStateException("Unknown Operating System: " + PlatformUtil.getOsName());
      }

      userName = (String) method.invoke(o);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }

    return new UserRoleInfo(userName);
  }

}
