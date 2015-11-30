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

package org.apache.tajo.util.metrics;

import com.codahale.metrics.ObjectNameFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class TajoJMXObjectNameFactory implements ObjectNameFactory {
  private static final Log LOG = LogFactory.getLog(TajoMetrics.class);
  private static String[] jmxHierarchies = new String[] {"type", "context"};
  private static String SEPARATOR_RGX = "\\.";

  @Override
  public ObjectName createName(String type, String domain, String name) {
    try {
      StringBuilder sb = new StringBuilder();
      sb.append(domain).append(":");

      String[] nameSplit = name.split(SEPARATOR_RGX, 3);
      ObjectName objectName = null;
      if (nameSplit.length == 1 ) {
        objectName = new ObjectName(domain, "name", name);
      } else {
        for (int i = 0; i < nameSplit.length - 1 && i < jmxHierarchies.length; i++) {
          sb.append(jmxHierarchies[i]).append("=").append(nameSplit[i]).append(",");
        }
        sb.append("name=").append(nameSplit[nameSplit.length - 1]);
        objectName = new ObjectName(sb.toString());
      }

      if (objectName.isPattern()) {
        objectName = new ObjectName(domain, "name", ObjectName.quote(name));
      }
      return objectName;
    } catch (MalformedObjectNameException e) {
      try {
        return new ObjectName(domain, "name", ObjectName.quote(name));
      } catch (MalformedObjectNameException e1) {
        if(LOG.isDebugEnabled()) {
          LOG.warn("Unable to register for " + type + " " + name + " " + e1.getMessage(), e1);
        } else {
          LOG.warn("Unable to register for " + type + " " + name + " " + e1.getMessage());
        }
        throw new RuntimeException(e1);
      }
    }
  }
}
