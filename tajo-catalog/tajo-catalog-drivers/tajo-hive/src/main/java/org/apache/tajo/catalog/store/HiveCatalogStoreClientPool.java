// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package org.apache.tajo.catalog.store;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages a pool of HiveMetaStoreClient connections. If the connection pool is empty
 * a new client is created and added to the pool. There is no size limit.
 */
public class HiveCatalogStoreClientPool {
  private static final Logger LOG = Logger.getLogger(HiveCatalogStoreClientPool.class);
  private final ConcurrentLinkedQueue<HiveCatalogStoreClient> clientPool =
          new ConcurrentLinkedQueue<>();
  private AtomicBoolean poolClosed = new AtomicBoolean(false);
  private HiveConf hiveConf;

  /**
   * A wrapper around the HiveMetaStoreClient that manages interactions with the
   * connection pool.
   */
  public class HiveCatalogStoreClient {
    private final IMetaStoreClient hiveClient;
    public AtomicBoolean isInUse = new AtomicBoolean(false);

    private HiveCatalogStoreClient(HiveConf hiveConf) {
      try {
        HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
          @Override
          public HiveMetaHook getHook(Table table) throws MetaException {
            /* metadata hook implementation, or null if this
             * storage handler does not need any metadata notifications
             */
            return null;
          }
        };

        this.hiveClient = RetryingMetaStoreClient.getProxy(hiveConf, hookLoader, HiveMetaStoreClient.class.getName());
        clientPool.add(this);
        LOG.info("MetaStoreClient created (size = " + clientPool.size() + ")");
      } catch (Exception e) {
        // Turn in to an unchecked exception
        throw new IllegalStateException(e);
      }
    }

    /**
     * Returns the internal HiveMetaStoreClient object.
     */
    public IMetaStoreClient getHiveClient() {
      return hiveClient;
    }

    /**
     * Returns this client back to the connection pool. If the connection pool has been
     * closed, just close the Hive client connection.
     */
    public synchronized void release() {
      if(!this.isInUse.getAndSet(false)){
        return;
      }
      // Ensure the connection isn't returned to the pool if the pool has been closed.
      // This lock is needed to ensure proper behavior when a thread reads poolClosed
      // is false, but a call to pool.close() comes in immediately afterward.
      if (poolClosed.get()) {
        this.getHiveClient().close();
      } else {
        clientPool.add(this);
      }
    }

    // Marks this client as in use
    private void markInUse() {
      isInUse.set(true);
    }
  }

  public HiveCatalogStoreClientPool(int initialSize) {
    this(initialSize, new HiveConf(HiveCatalogStoreClientPool.class));
  }

  public HiveCatalogStoreClientPool(int initialSize, HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    addClients(initialSize);
  }

  public HiveCatalogStoreClientPool(int initialSize, Configuration conf) {
    this.hiveConf = new HiveConf();
    setParameters(conf);
    addClients(initialSize);
  }

  public void setParameters(Configuration conf) {
    for (Entry<String, String> entry : conf) {
      this.hiveConf.set(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Add numClients to the client pool.
   */
  public void addClients(int numClients) {
    for (int i = 0; i < numClients; ++i) {
      clientPool.add(new HiveCatalogStoreClient(hiveConf));
    }
  }

  /**
   * Gets a client from the pool. If the pool is empty a new client is created.
   */
  public synchronized HiveCatalogStoreClient getClient() {
    // The MetaStoreClient c'tor relies on knowing the Hadoop version by asking
    // org.apache.hadoop.util.VersionInfo. The VersionInfo class relies on opening
    // the 'common-version-info.properties' file as a resource from hadoop-common*.jar
    // using the Thread's context classloader. If necessary, set the Thread's context
    // classloader, otherwise VersionInfo will fail in it's c'tor.
    if (Thread.currentThread().getContextClassLoader() == null) {
      Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
    }

    HiveCatalogStoreClient client = clientPool.poll();
    // The pool was empty so create a new client and return that.
    if (client == null) {
      client = new HiveCatalogStoreClient(hiveConf);
    }
    client.markInUse();

    return client;
  }

  /**
   * Removes all items from the connection pool and closes all Hive Meta Store client
   * connections. Can be called multiple times.
   */
  public void close() {
    // Ensure no more items get added to the pool once close is called.
    if (poolClosed.getAndSet(true)) {
      return;
    }

    HiveCatalogStoreClient client = null;
    while ((client = clientPool.poll()) != null) {
      client.getHiveClient().close();
    }
  }
}
