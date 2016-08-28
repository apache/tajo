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

package org.apache.tajo.storage.kafka.server;

import static io.airlift.testing.FileUtils.deleteRecursively;

import org.apache.tajo.util.NetUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.io.Files;

public class EmbeddedZookeeper implements Closeable {
  private final File zkDataDir;
  private final ZooKeeperServer zkServer;
  private final NIOServerCnxnFactory cnxnFactory;

  private final AtomicBoolean started = new AtomicBoolean();

  public EmbeddedZookeeper() throws IOException {
    this(2181);
  }

  public EmbeddedZookeeper(int port) throws IOException {
    zkDataDir = Files.createTempDir();
    zkServer = new ZooKeeperServer();

    FileTxnSnapLog ftxn = new FileTxnSnapLog(zkDataDir, zkDataDir);
    zkServer.setTxnLogFactory(ftxn);

    cnxnFactory = new NIOServerCnxnFactory();
    cnxnFactory.configure(new InetSocketAddress(port), 0);
  }

  public void start() throws InterruptedException, IOException {
    if (!started.getAndSet(true)) {
      cnxnFactory.startup(zkServer);
    }
  }

  @Override
  public void close() {
    if (started.get()) {
      cnxnFactory.shutdown();
      try {
        cnxnFactory.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      if (zkServer.isRunning()) {
        zkServer.shutdown();
      }
      deleteRecursively(zkDataDir);
    }
  }

  public String getConnectString() {
    return NetUtils.normalizeInetSocketAddress(cnxnFactory.getLocalAddress());
  }
}
