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

import org.apache.hadoop.net.NetUtils;

import javax.net.SocketFactory;
import java.net.InetSocketAddress;
import java.net.Socket;

public abstract class HAServiceTracker implements ServiceTracker {

  static SocketFactory socketFactory = SocketFactory.getDefault();

  public boolean isHighAvailable() {
    return true;
  }

  public static boolean checkConnection(InetSocketAddress address) {
    boolean isAlive = true;

    try {
      int connectionTimeout = 10;

      Socket socket = socketFactory.createSocket();
      NetUtils.connect(socket, address, connectionTimeout);
    } catch (Exception e) {
      isAlive = false;
    }
    return isAlive;
  }
}
