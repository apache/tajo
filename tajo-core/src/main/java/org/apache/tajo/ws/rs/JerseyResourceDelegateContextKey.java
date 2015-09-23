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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.ws.rs.core.GenericType;

public class JerseyResourceDelegateContextKey<T> {
  
  private static final ConcurrentMap<String, JerseyResourceDelegateContextKey<?>> keyMap =
          new ConcurrentHashMap<>();
  
  private final String name;
  private final Class<T> type;
  
  private JerseyResourceDelegateContextKey(String name, Class<T> type) {
    this.name = name;
    this.type = type;
  }
  
  public static <T> JerseyResourceDelegateContextKey<T> valueOf(String name, Class<T> type) {
    if (name == null || name.isEmpty()) {
      throw new RuntimeException("name cannnot be null or empty.");
    }
    
    JerseyResourceDelegateContextKey<T> key = (JerseyResourceDelegateContextKey<T>) keyMap.get(name);
    if (key == null) {
      key = new JerseyResourceDelegateContextKey<>(name, type);
      keyMap.putIfAbsent(name, key);
    }
    
    return key;
  }
  
  @SuppressWarnings("unchecked")
  public static <T> JerseyResourceDelegateContextKey<T> valueOf(String name, GenericType<T> genericType) {
    return (JerseyResourceDelegateContextKey<T>) valueOf(name, genericType.getRawType());
  }
  
  public Class<T> getType() {
    return type;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    JerseyResourceDelegateContextKey other = (JerseyResourceDelegateContextKey) obj;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return true;
  }
  
}
