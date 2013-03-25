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

package tajo.rpc.benchmark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;
import java.net.InetSocketAddress;

public class BenchmarkHadoopRPC {

  public static class ClientWrapper extends Thread {
    @SuppressWarnings("unused")
    public void run() {
      InetSocketAddress addr = new InetSocketAddress("localhost", 15000);
      BenchmarkInterface proxy = null;
      try {
        proxy =
            RPC.waitForProxy(BenchmarkInterface.class, 1,
                addr, new Configuration());
      } catch (IOException e1) {
        e1.printStackTrace();
      }

      long start = System.currentTimeMillis();
      for (int i = 0; i < 100000; i++) {
        proxy.shoot("ABCD");
      }
      long end = System.currentTimeMillis();
      System.out.println("elapsed time: " + (end - start) + "msc");
    }
  }

  public static interface BenchmarkInterface extends VersionedProtocol {
    public String shoot(String l);
  }

  public static class BenchmarkImpl implements BenchmarkInterface {
    @Override
    public String shoot(String l) {
      return l;
    }

    @Override
    public long getProtocolVersion(String arg0, long arg1) throws IOException {
      return 1l;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
                                                  long clientVersion, int clientMethodsHash) throws IOException {
      ProtocolSignature ps = null;
      try {
        ps = ProtocolSignature.getProtocolSignature("benchmarkInterface", 0);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }

      return ps;
    }
  }

  public static void main(String[] args) throws InterruptedException,
      IOException {
    Server server =
        RPC.getServer(new BenchmarkImpl(), "localhost", 15000,
            new Configuration());
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

    server.stop();
  }
}
