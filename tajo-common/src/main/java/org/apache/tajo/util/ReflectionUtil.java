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

package org.apache.tajo.util;

import org.apache.tajo.conf.TajoConf;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReflectionUtil {
  public static final Class<?>[] EMPTY_PARAM = new Class[]{};
  public static final Object [] EMPTY_OBJECT = new Object[] {};
  public static final Class<?>[] CONF_PARAM = new Class[]{TajoConf.class};

  /**
   * Caches of constructors for each class. Pins the classes so they
   * can't be garbage collected until ReflectionUtils can be collected.
   *
   * EMPTY_CONSTRUCTOR_CACHE keeps classes which don't have any parameterized constructor, and
   * CONF_CONSTRUCTOR_CACHE keeps classes which have one constructor to take TajoConf.
   */
  private static final Map<Class<?>, Constructor<?>> EMPTY_CONSTRUCTOR_CACHE =
      new ConcurrentHashMap<Class<?>, Constructor<?>>();
  private static final Map<Class<?>, Constructor<?>> CONF_CONSTRUCTOR_CACHE =
      new ConcurrentHashMap<Class<?>, Constructor<?>>();

  /**
   * Initialize an instance by a given class
   *
   * @param clazz Class to be initialized
   * @return initialized object
   */
	public static <T> T newInstance(Class<? extends T> clazz) {
    try {
      Constructor<?> constructor;
      if (EMPTY_CONSTRUCTOR_CACHE.containsKey(clazz)) {
        constructor = EMPTY_CONSTRUCTOR_CACHE.get(clazz);
      } else {
        constructor = clazz.getConstructor(EMPTY_PARAM);
        EMPTY_CONSTRUCTOR_CACHE.put(clazz, constructor);
      }

      return (T) constructor.newInstance(EMPTY_OBJECT);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
	}

  /**
   * Initialize an instance by a given class with TajoConf parameter
   *
   * @param clazz Class to be initialized
   * @param conf TajoConf instance
   * @return initialized object
   */
  public static <T> T newInstance(Class<? extends T> clazz, TajoConf conf) {
    try {
      Constructor<?> constructor;
      if (CONF_CONSTRUCTOR_CACHE.containsKey(clazz)) {
        constructor = CONF_CONSTRUCTOR_CACHE.get(clazz);
      } else {
        constructor = clazz.getConstructor(CONF_PARAM);
        CONF_CONSTRUCTOR_CACHE.put(clazz, constructor);
      }

      return (T) constructor.newInstance(new Object[]{conf});
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }
}
