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

public class RpcConnectionKey {
  final InetSocketAddress addr;
  final Class<?> protocolClass;
  final boolean asyncMode;

  final String description;

  public RpcConnectionKey(InetSocketAddress addr,
                          Class<?> protocolClass, boolean asyncMode) {
    this.addr = addr;
    this.protocolClass = protocolClass;
    this.asyncMode = asyncMode;
    this.description = "[" + protocolClass + "] " + addr + "," + asyncMode;
  }

  @Override
  public String toString() {
    return description;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof RpcConnectionKey)) {
      return false;
    }

    return toString().equals(obj.toString());
  }

  @Override
  public int hashCode() {
    return description.hashCode();
  }
}
