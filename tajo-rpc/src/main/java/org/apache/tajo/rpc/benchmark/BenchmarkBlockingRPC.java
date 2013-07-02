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

package org.apache.tajo.rpc.benchmark;

import org.apache.tajo.rpc.NettyRpc;
import org.apache.tajo.rpc.NettyRpcServer;
import org.apache.tajo.rpc.RemoteException;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;

import java.net.InetSocketAddress;

public class BenchmarkBlockingRPC {

  public static class ClientWrapper extends Thread {
    @SuppressWarnings("unused")
    public void run() {
      InetSocketAddress addr = new InetSocketAddress("localhost", 15001);
      BenchmarkInterface proxy;
      proxy =
          (BenchmarkInterface) NettyRpc.getProtoParamBlockingRpcProxy(
              BenchmarkInterface.class, addr);

      long start = System.currentTimeMillis();
      StringProto ps = StringProto.newBuilder().setValue("ABCD").build();
      for (int i = 0; i < 100000; i++) {
        try {
          proxy.shoot(ps);
        } catch (RemoteException e1) {
          System.out.println(e1.getMessage());
        }
      }
      long end = System.currentTimeMillis();
      System.out.println("elapsed time: " + (end - start) + "msc");

    }
  }

  public static interface BenchmarkInterface {
    public StringProto shoot(StringProto l) throws RemoteException;
  }

  public static class BenchmarkImpl implements BenchmarkInterface {
    @Override
    public StringProto shoot(StringProto l) {
      return l;
    }
  }

  public static void main(String[] args) throws InterruptedException,
      RemoteException {

    NettyRpcServer server =
        NettyRpc
            .getProtoParamRpcServer(new BenchmarkImpl(),
                BenchmarkInterface.class, new InetSocketAddress("localhost",
                    15001));

    server.start();
    Thread.sleep(1000);

    int numThreads = 1;
    ClientWrapper client[] = new ClientWrapper[numThreads];
    for (int i = 0; i < numThreads; i++) {
      client[i] = new ClientWrapper();
    }

    for (int i = 0; i < numThreads; i++) {
      client[i].start();
    }

    for (int i = 0; i < numThreads; i++) {
      client[i].join();
    }

    server.shutdown();
    System.exit(0);
  }
}
