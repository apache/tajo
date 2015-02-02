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

package org.apache.tajo.service;

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.ReflectionUtil;

public class ServiceTrackerFactory {

  public static ServiceTracker get(TajoConf conf) {
    Class<ServiceTracker> trackerClass;

    try {
      if (conf.getBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE)) {
        trackerClass = (Class<ServiceTracker>) conf.getClassVar(TajoConf.ConfVars.HA_SERVICE_TRACKER_CLASS);
      } else {
        trackerClass = (Class<ServiceTracker>) conf.getClassVar(TajoConf.ConfVars.DEFAULT_SERVICE_TRACKER_CLASS);
      }
      return ReflectionUtil.newInstance(trackerClass, conf);

    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }
}
