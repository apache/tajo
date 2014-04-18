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

package org.apache.tajo.master;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;

public class FragmentScheduleAlgorithmFactory {

  private static Class<? extends FragmentScheduleAlgorithm> CACHED_ALGORITHM_CLASS;
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = Maps.newConcurrentMap();
  private static final Class<?>[] DEFAULT_PARAMS = {};

  public static Class<? extends FragmentScheduleAlgorithm> getScheduleAlgorithmClass(Configuration conf)
      throws IOException {
    if (CACHED_ALGORITHM_CLASS != null) {
      return CACHED_ALGORITHM_CLASS;
    } else {
      CACHED_ALGORITHM_CLASS = conf.getClass("tajo.querymaster.lazy-task-scheduler.algorithm", null,
          FragmentScheduleAlgorithm.class);
    }

    if (CACHED_ALGORITHM_CLASS == null) {
      throw new IOException("Scheduler algorithm is null");
    }
    return CACHED_ALGORITHM_CLASS;
  }

  public static <T extends FragmentScheduleAlgorithm> T get(Class<T> clazz) {
    T result;
    try {
      Constructor<T> constructor = (Constructor<T>) CONSTRUCTOR_CACHE.get(clazz);
      if (constructor == null) {
        constructor = clazz.getDeclaredConstructor(DEFAULT_PARAMS);
        constructor.setAccessible(true);
        CONSTRUCTOR_CACHE.put(clazz, constructor);
      }
      result = constructor.newInstance(new Object[]{});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static FragmentScheduleAlgorithm get(Configuration conf) throws IOException {
    return get(getScheduleAlgorithmClass(conf));
  }
}
