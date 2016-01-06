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

package org.apache.tajo.ws.rs.resources.outputs;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.master.exec.NonForwardQueryResultScanner;
import org.apache.tajo.util.ClassUtil;
import org.apache.tajo.ws.rs.annotation.RestReturnType;

import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;

public class RestOutputFactory {
  private static Log LOG = LogFactory.getLog(RestOutputFactory.class);
  private static Map<String, String> restOutputClasses = load();

  private static Map<String, String> load() {
    Map<String, String> outputClasses = Maps.newHashMap();
    Set<Class> restOutputClasses = ClassUtil.findClasses(AbstractStreamingOutput.class, "org.apache.tajo.ws.rs.resources.outputs");

    for (Class eachClass : restOutputClasses) {
      if (eachClass.isInterface() || 
          Modifier.isAbstract(eachClass.getModifiers())) {
        continue;
      }

      AbstractStreamingOutput streamingOutput = null;
      try {
        streamingOutput = (AbstractStreamingOutput) eachClass.getDeclaredConstructor(
        new Class[]{NonForwardQueryResultScanner.class, Integer.class, Integer.class}).newInstance(null, 0, 0);
      } catch (Exception e) {
        LOG.warn(eachClass + " cannot instantiate Function class because of " + e.getMessage(), e);
        continue;
      }
      String className = streamingOutput.getClass().getCanonicalName();
      String headerType = streamingOutput.getClass().getAnnotation(RestReturnType.class).mimeType();

      if (StringUtils.isNotEmpty(headerType)) {
        outputClasses.put(headerType, className);
      }
    }

    return outputClasses;
  }

  public static AbstractStreamingOutput get(String mimeType, NonForwardQueryResultScanner scanner, Integer count, Integer startOffset) {
    AbstractStreamingOutput output = null;
    try {
      if (restOutputClasses.containsKey(mimeType)) {
        String className = (String) restOutputClasses.get(mimeType);
        Class<?> clazz = Class.forName(className);
        output = (AbstractStreamingOutput) clazz.getDeclaredConstructor(
                  new Class[]{NonForwardQueryResultScanner.class,
                  Integer.class, Integer.class})
                  .newInstance(scanner, count, startOffset);
      } else {
        output = new CSVStreamingOutput(scanner, count, startOffset);
      }
    } catch (Exception eh) {
        LOG.error(eh);
    }

    return output;
  }
}
