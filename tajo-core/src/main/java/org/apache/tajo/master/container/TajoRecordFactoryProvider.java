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

package org.apache.tajo.master.container;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * This class is borrowed from the following source code :
 * ${hadoop-yarn-api}/src/main/java/org/apache/hadoop/yarn/factory/providers/RecordFactoryProvider.java
 */
@Unstable
public class TajoRecordFactoryProvider {
  private static Configuration defaultConf;

  static {
    defaultConf = new Configuration();
  }

  private TajoRecordFactoryProvider() {
  }

  public static TajoRecordFactory getRecordFactory(Configuration conf) {
    if (conf == null) {
      //Assuming the default configuration has the correct factories set.
      //Users can specify a particular factory by providing a configuration.
      conf = defaultConf;
    }
    return (TajoRecordFactory) getFactoryClassInstance(TajoRecordFactoryPBImpl.class.getCanonicalName());
  }

  private static Object getFactoryClassInstance(String factoryClassName) {
    try {
      Class<?> clazz = Class.forName(factoryClassName);
      Method method = clazz.getMethod("get", null);
      method.setAccessible(true);
      return method.invoke(null, null);
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new YarnRuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new YarnRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new YarnRuntimeException(e);
    }
  }
}
