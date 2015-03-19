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

package org.apache.tajo.rpc;

import java.net.InetSocketAddress;

/**
 * blocking server for client access
 * Only should be used for QueryMasterClientProtocol or TajoMasterClientProtocol or CatalogProtocol
 */
public interface PublicServiceProvider<T> {

  void start() throws Exception;

  void shutdown() throws Exception;

  void addListener(RpcEventListener listener);

  void removeListener(RpcEventListener listener);

  InetSocketAddress getListenAddress();

  String getServiceName();

  static class Utils {
    Class<?> toServiceClassClass(Class<?> protocolClass) throws Exception {
      return Class.forName(protocolClass.getName() + "$" + protocolClass.getSimpleName() + "Service");
    }
    Class<?> toInterfaceClass(Class<?> protocolClass) throws Exception {
      return Class.forName(toServiceClassClass(protocolClass).getName() + "$BlockingInterface");
    }
  }
}
