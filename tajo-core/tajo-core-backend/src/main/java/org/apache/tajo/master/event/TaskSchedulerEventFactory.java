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

package org.apache.tajo.master.event;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.master.TaskSchedulerFactory;
import org.apache.tajo.master.event.TaskSchedulerEvent.EventType;
import org.apache.tajo.master.querymaster.QueryUnitAttempt;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;

public class TaskSchedulerEventFactory {
  private static final Map<String, Class<? extends TaskSchedulerEvent>> CACHED_EVENT_CLASSES = Maps.newConcurrentMap();
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = Maps.newConcurrentMap();
  private static final Class<?>[] DEFAULT_EVENT_PARAMS = { EventType.class, QueryUnitAttempt.class };

  public static <T extends TaskSchedulerEvent> T getTaskSchedulerEvent(Configuration conf, QueryUnitAttempt attempt, EventType eventType) {
    T result;

    try {
      Class<T> eventClass = (Class<T>) getTaskSchedulerEventClass(conf);
      Constructor<T> constructor = (Constructor<T>) CONSTRUCTOR_CACHE.get(eventClass);
      if (constructor == null) {
        constructor = eventClass.getDeclaredConstructor(DEFAULT_EVENT_PARAMS);
        constructor.setAccessible(true);
        CONSTRUCTOR_CACHE.put(eventClass, constructor);
      }
      result = constructor.newInstance(new Object[]{ eventType, attempt });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static Class<? extends TaskSchedulerEvent> getTaskSchedulerEventClass(Configuration conf) throws IOException {
    String handlerName = TaskSchedulerFactory.getSchedulerType(conf);
    Class<? extends TaskSchedulerEvent> eventClass = CACHED_EVENT_CLASSES.get(handlerName);
    if (eventClass == null) {
      eventClass = conf.getClass(String.format("tajo.querymaster.task-schedule-event.%s.class", handlerName), null, TaskSchedulerEvent.class);
      CACHED_EVENT_CLASSES.put(handlerName, eventClass);
    }

    if (eventClass == null) {
      throw new IOException("Unknown Event Type: " + handlerName);
    }
    return eventClass;
  }
}
