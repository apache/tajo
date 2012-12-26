/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.rpc;

import java.net.InetSocketAddress;

public class NettyRpc {
  @Deprecated
  public static NettyRpcServer getProtoParamRpcServer(Object instance,
      Class<?> interfaceClass, InetSocketAddress addr) {
    return new NettyRpcServer(instance, interfaceClass, addr);
  }

  @Deprecated
  public static Object getProtoParamAsyncRpcProxy(Class<?> serverClass,
      Class<?> clientClass, InetSocketAddress addr) {
    return new NettyAsyncRpcProxy(serverClass, clientClass, addr)
        .getProxy();
  }

  @Deprecated
  public static Object getProtoParamBlockingRpcProxy(Class<?> protocol,
      InetSocketAddress addr) {
    return new NettyBlockingRpcProxy(protocol, addr).getProxy();
  }
}
