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

package org.apache.tajo.ws.rs;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * It holds variables for running deletegates
 */
public class JerseyResourceDelegateContext {

  private final ConcurrentMap<JerseyResourceDelegateContextKey<?>, Object> contextMap =
          new ConcurrentHashMap<>();
  
  /**
   * Add value to Context. If value exists, it will overwrite.
   * 
   * @param key
   * @param value
   * @return
   */
  public <T> JerseyResourceDelegateContext put(JerseyResourceDelegateContextKey<T> key, T value) {
    contextMap.put(key, value);
    return this;
  }
  
  public <T> T get(JerseyResourceDelegateContextKey<T> key) {
    Class<T> keyTypeClass = key.getType();
    Object object = contextMap.get(key);
    if (object != null) {
      return keyTypeClass.cast(contextMap.get(key));
    } else {
      if (Boolean.class.isAssignableFrom(keyTypeClass)) {
        return keyTypeClass.cast(false);
      } else if (Number.class.isAssignableFrom(keyTypeClass)) {
        try {
          return keyTypeClass.getConstructor(String.class).newInstance("0");
        } catch (Throwable e) {
          return null;
        }
      } else {
        return null;
      }
    }
  }
}
