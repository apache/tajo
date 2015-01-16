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


import static org.apache.hadoop.yarn.util.StringHelper._split;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;


/**
 * This class is borrowed from the following source code :
 * ${hadoop-yarn-common}/src/main/java/org/apache/hadoop/yarn/util/ConverterUtils.java
 *
 * This class contains a set of utilities which help converting data structures
 * from/to 'serializableFormat' to/from hadoop/nativejava data structures.
 *
 */
@Private
public class TajoConverterUtils {

  public static final String CONTAINER_PREFIX = "container";

  private static ApplicationAttemptId toApplicationAttemptId(
    Iterator<String> it) throws NumberFormatException {
    ApplicationId appId = ApplicationId.newInstance(Long.parseLong(it.next()),
      Integer.parseInt(it.next()));
    ApplicationAttemptId appAttemptId =
      ApplicationAttemptId.newInstance(appId, Integer.parseInt(it.next()));
    return appAttemptId;
  }

  public static String toString(TajoContainerId cId) {
    return cId == null ? null : cId.toString();
  }

  public static TajoContainerId toTajoContainerId(String containerIdStr) {
    Iterator<String> it = _split(containerIdStr).iterator();
    if (!it.next().equals(CONTAINER_PREFIX)) {
      throw new IllegalArgumentException("Invalid TajoContainerId prefix: "
        + containerIdStr);
    }
    try {
      ApplicationAttemptId appAttemptID = toApplicationAttemptId(it);
      TajoContainerId containerId =
        TajoContainerId.newInstance(appAttemptID, Integer.parseInt(it.next()));
      return containerId;
    } catch (NumberFormatException n) {
      throw new IllegalArgumentException("Invalid TajoContainerId: "
        + containerIdStr, n);
    }
  }
}
