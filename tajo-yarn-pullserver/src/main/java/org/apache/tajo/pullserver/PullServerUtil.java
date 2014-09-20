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

import org.apache.commons.lang.reflect.MethodUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.nativeio.NativeIO;

import java.io.FileDescriptor;
import java.lang.reflect.Method;

public class PullServerUtil {
  private static final Log LOG = LogFactory.getLog(PullServerUtil.class);

  private static boolean nativeIOPossible = false;
  private static Method posixFadviseIfPossible;

  static {
    if (NativeIO.isAvailable() && loadNativeIO()) {
      nativeIOPossible = true;
    } else {
      LOG.warn("Unable to load hadoop nativeIO");
    }
  }

  public static boolean isNativeIOPossible() {
    return nativeIOPossible;
  }

  /**
   * Call posix_fadvise on the given file descriptor. See the manpage
   * for this syscall for more information. On systems where this
   * call is not available, does nothing.
   */
  public static void posixFadviseIfPossible(String identifier, java.io.FileDescriptor fd,
                                            long offset, long len, int flags) {
    if (nativeIOPossible) {
      try {
        posixFadviseIfPossible.invoke(null, identifier, fd, offset, len, flags);
      } catch (Throwable t) {
        nativeIOPossible = false;
        LOG.warn("Failed to manage OS cache for " + identifier, t);
      }
    }
  }

  /* load hadoop native method if possible */
  private static boolean loadNativeIO() {
    boolean loaded = true;
    if (nativeIOPossible) return loaded;

    Class[] parameters = {String.class, FileDescriptor.class, Long.TYPE, Long.TYPE, Integer.TYPE};
    try {
      Method getCacheManipulator = MethodUtils.getAccessibleMethod(NativeIO.POSIX.class, "getCacheManipulator", new Class[0]);
      Class posixClass;
      if (getCacheManipulator != null) {
        Object posix = MethodUtils.invokeStaticMethod(NativeIO.POSIX.class, "getCacheManipulator", null);
        posixClass = posix.getClass();
      } else {
        posixClass = NativeIO.POSIX.class;
      }
      posixFadviseIfPossible = MethodUtils.getAccessibleMethod(posixClass, "posixFadviseIfPossible", parameters);
    } catch (Throwable e) {
      loaded = false;
      LOG.warn("Failed to access posixFadviseIfPossible :" + e.getMessage());
    }

    if (posixFadviseIfPossible == null) {
      loaded = false;
    }
    return loaded;
  }
}
