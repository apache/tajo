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
import org.apache.tajo.master.querymaster.QueryMasterTask.QueryMasterTaskContext;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;

public class TaskSchedulerFactory {

  private static final Map<String, Class<? extends AbstractTaskScheduler>> CACHED_SCHEDULER_CLASSES = Maps.newConcurrentMap();

  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = Maps.newConcurrentMap();

  private static final Class<?>[] DEFAULT_SCHEDULER_PARAMS = { QueryMasterTaskContext.class };

  public static <T extends AbstractTaskScheduler> T getTaskSCheduler(Configuration conf, QueryMasterTaskContext context) {
    T result;

    try {
      Class<T> schedulerClass = (Class<T>) getTaskSchedulerClass(conf);
      Constructor<T> constructor = (Constructor<T>) CONSTRUCTOR_CACHE.get(schedulerClass);
      if (constructor == null) {
        constructor = schedulerClass.getDeclaredConstructor(DEFAULT_SCHEDULER_PARAMS);
        constructor.setAccessible(true);
        CONSTRUCTOR_CACHE.put(schedulerClass, constructor);
      }
      result = constructor.newInstance(new Object[]{context});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return result;
  }

  public static Class<? extends AbstractTaskScheduler> getTaskSchedulerClass(Configuration conf) throws IOException {
    String handlerName = getSchedulerType(conf);
    Class<? extends AbstractTaskScheduler> schedulerClass = CACHED_SCHEDULER_CLASSES.get(handlerName);
    if (schedulerClass == null) {
      schedulerClass = conf.getClass(String.format("tajo.querymaster.scheduler-handler.%s.class", handlerName), null, AbstractTaskScheduler.class);
      CACHED_SCHEDULER_CLASSES.put(handlerName, schedulerClass);
    }

    if (schedulerClass == null) {
      throw new IOException("Unknown Scheduler Type: " + handlerName);
    }

    return schedulerClass;
  }

  public static String getSchedulerType(Configuration conf) {
    return conf.get("tajo.querymaster.scheduler-handler.type", "default");
  }
}
