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

package org.apache.tajo.querymaster;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;

public class TaskSchedulerFactory {
  private static Class<? extends AbstractTaskScheduler> CACHED_ALGORITHM_CLASS;
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = Maps.newConcurrentMap();
  private static final Class<?>[] DEFAULT_PARAMS = { TaskSchedulerContext.class, Stage.class };

  public static Class<? extends AbstractTaskScheduler> getTaskSchedulerClass(Configuration conf)
      throws IOException {
    if (CACHED_ALGORITHM_CLASS != null) {
      return CACHED_ALGORITHM_CLASS;
    } else {
      CACHED_ALGORITHM_CLASS = conf.getClass("tajo.querymaster.task-scheduler", null, AbstractTaskScheduler.class);
    }

    if (CACHED_ALGORITHM_CLASS == null) {
      throw new IOException("Task scheduler is null");
    }
    return CACHED_ALGORITHM_CLASS;
  }

  public static <T extends AbstractTaskScheduler> T get(Class<T> clazz, TaskSchedulerContext context,
                                                        Stage stage) {
    T result;
    try {
      Constructor<T> constructor = (Constructor<T>) CONSTRUCTOR_CACHE.get(clazz);
      if (constructor == null) {
        constructor = clazz.getDeclaredConstructor(DEFAULT_PARAMS);
        constructor.setAccessible(true);
        CONSTRUCTOR_CACHE.put(clazz, constructor);
      }
      result = constructor.newInstance(new Object[]{context, stage});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static AbstractTaskScheduler get(Configuration conf, TaskSchedulerContext context, Stage stage)
      throws IOException {
    return get(getTaskSchedulerClass(conf), context, stage);
  }
}
