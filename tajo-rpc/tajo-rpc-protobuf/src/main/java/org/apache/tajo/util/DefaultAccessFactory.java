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

import org.apache.tajo.rpc.BlockingRpcServer;
import org.apache.tajo.rpc.PublicServiceFactory;
import org.apache.tajo.rpc.PublicServiceProvider;
import org.apache.tajo.rpc.RpcUtils;

import java.net.InetSocketAddress;

public class DefaultAccessFactory implements PublicServiceFactory {

  private static final PublicServiceFactory DEFAULT = new DefaultAccessFactory();

  public static PublicServiceFactory newFactory(String className) throws Exception {
    if (className != null && !className.isEmpty()) {
      return (PublicServiceFactory)Class.forName(className).newInstance();
    }
    return DEFAULT;
  }

  @Override
  public PublicServiceProvider create(String serviceURL, Class<?> protocolClass,
                                     Object serviceHandler, int workerNum) throws Exception {
    InetSocketAddress initIsa = RpcUtils.createSocketAddr(serviceURL);
    return new BlockingRpcServer(protocolClass, serviceHandler, initIsa, workerNum);
  }
}
