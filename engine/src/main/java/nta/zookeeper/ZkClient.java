package nta.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import nta.engine.NConstants;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZkClient implements Watcher {
  private final Log LOG = LogFactory.getLog(ZkClient.class);

  private CountDownLatch latch = new CountDownLatch(1);

  private ZooKeeper zk;
  private RetryCounterFactory retryCounterFactory;

  private final List<ZkListener> listeners =
      new CopyOnWriteArrayList<ZkListener>();

  public ZkClient(Configuration conf) throws IOException {
    this(conf.get(NConstants.ZOOKEEPER_ADDRESS,
        NConstants.DEFAULT_ZOOKEEPER_ADDRESS), conf.getInt(
        NConstants.ZOOKEEPER_SESSION_TIMEOUT,
        NConstants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT), conf.getInt(
        NConstants.ZOOKEEPER_RETRY_COUNT,
        NConstants.DEFAULT_ZOOKEEPER_RETRY_COUNT), conf.getInt(
        NConstants.ZOOKEEPER_RETRY_INTERVALMILLS,
        NConstants.DEFAULT_ZOOKEEPER_RETRY_INTERVALMILLS));
  }

  public ZkClient(String serverstring) throws IOException {
    this(serverstring, NConstants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT,
        NConstants.DEFAULT_ZOOKEEPER_RETRY_COUNT,
        NConstants.DEFAULT_ZOOKEEPER_RETRY_INTERVALMILLS);
  }

  public ZkClient(String serverstring, int sessionTimeout) throws IOException {
    this(serverstring, sessionTimeout,
        NConstants.DEFAULT_ZOOKEEPER_RETRY_COUNT,
        NConstants.DEFAULT_ZOOKEEPER_RETRY_INTERVALMILLS);
  }

  public ZkClient(String serverstring, int sessionTimeout, int maxRetries,
      int retryIntervalMills) throws IOException {
    this.zk = new ZooKeeper(serverstring, sessionTimeout, this);
    this.retryCounterFactory =
        new RetryCounterFactory(maxRetries, retryIntervalMills);
    try {
      this.latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void subscribe(ZkListener listener) {
    this.listeners.add(listener);
  }

  public void unsubscribe(ZkListener listener) {
    this.listeners.remove(listener);
  }

  public void createPersistent(String path) throws KeeperException,
      InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        return;
      } catch (KeeperException e) {
        switch (e.code()) {
        case CONNECTIONLOSS:
        case OPERATIONTIMEOUT:
          LOG.warn("Possibly transient ZooKeeper exception: " + e);
          if (!retryCounter.shouldRetry()) {
            LOG.error("ZooKeeper create failed after "
                + retryCounter.getMaxRetries() + " retries");
            throw e;
          }
          break;

        default:
          throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  public void createPersistent(final String path, final byte[] data)
      throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        return;
      } catch (KeeperException e) {
        switch (e.code()) {
        case CONNECTIONLOSS:
        case OPERATIONTIMEOUT:
          LOG.warn("Possibly transient ZooKeeper exception: " + e);
          if (!retryCounter.shouldRetry()) {
            LOG.error("ZooKeeper create failed after "
                + retryCounter.getMaxRetries() + " retries");
            throw e;
          }
          break;

        default:
          throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  public void createEphemeral(final String path) throws KeeperException,
      InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        return;
      } catch (KeeperException e) {
        switch (e.code()) {
        case CONNECTIONLOSS:
        case OPERATIONTIMEOUT:
          LOG.warn("Possibly transient ZooKeeper exception: " + e);
          if (!retryCounter.shouldRetry()) {
            LOG.error("ZooKeeper create failed after "
                + retryCounter.getMaxRetries() + " retries");
            throw e;
          }
          break;

        default:
          throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  public void createEphemeral(final String path, final byte[] data)
      throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        return;
      } catch (KeeperException e) {
        switch (e.code()) {
        case CONNECTIONLOSS:
        case OPERATIONTIMEOUT:
          LOG.warn("Possibly transient ZooKeeper exception: " + e);
          if (!retryCounter.shouldRetry()) {
            LOG.error("ZooKeeper create failed after "
                + retryCounter.getMaxRetries() + " retries");
            throw e;
          }
          break;

        default:
          throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  public String createEphemeralSequential(final String path, final byte[] data)
      throws Exception {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.create(path, data, Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL);
      } catch (KeeperException e) {
        switch (e.code()) {
        case CONNECTIONLOSS:
        case OPERATIONTIMEOUT:
          LOG.warn("Possibly transient ZooKeeper exception: " + e);
          if (!retryCounter.shouldRetry()) {
            LOG.error("ZooKeeper create failed after "
                + retryCounter.getMaxRetries() + " retries");
            throw e;
          }
          break;

        default:
          throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  public Stat exists(final String path) throws KeeperException,
      InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.exists(path, true);
      } catch (KeeperException e) {
        switch (e.code()) {
        case SESSIONEXPIRED:
        case CONNECTIONLOSS:
        case OPERATIONTIMEOUT:
          LOG.warn("Possibly transient ZooKeeper exception: " + e);
          if (!retryCounter.shouldRetry()) {
            LOG.error("ZooKeeper exists failed after "
                + retryCounter.getMaxRetries() + " retries");
            throw e;
          }
          break;

        default:
          throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  public Stat exists(final String path, boolean watch) throws KeeperException,
      InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.exists(path, watch);
      } catch (KeeperException e) {
        switch (e.code()) {
        case CONNECTIONLOSS:
        case OPERATIONTIMEOUT:
          LOG.warn("Possibly transient ZooKeeper exception: " + e);
          if (!retryCounter.shouldRetry()) {
            LOG.error("ZooKeeper exists failed after "
                + retryCounter.getMaxRetries() + " retries");
            throw e;
          }
          break;

        default:
          throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  public void delete(final String path) throws InterruptedException,
      KeeperException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        zk.delete(path, -1);
        return;
      } catch (KeeperException e) {
        switch (e.code()) {
        case CONNECTIONLOSS:
        case OPERATIONTIMEOUT:
          LOG.warn("Possibly transient ZooKeeper exception: " + e);
          if (!retryCounter.shouldRetry()) {
            LOG.error("ZooKeeper delete failed after "
                + retryCounter.getMaxRetries() + " retries");
            throw e;
          }
          break;

        default:
          throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  @Override
  public void process(WatchedEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received Zookeeper Event, " + "type=" + event.getType() + ", "
          + "state=" + event.getState() + ", " + "path=" + event.getPath());
    }

    switch (event.getType()) {
    case None: {
      if (event.getState() == KeeperState.SyncConnected) {
        latch.countDown();
      }
    }

    case NodeCreated: {
      for (ZkListener listener : this.listeners) {
        listener.nodeCreated(event.getPath());
      }
      break;
    }

    case NodeDeleted: {
      for (ZkListener listener : this.listeners) {
        listener.nodeDeleted(event.getPath());
      }
      break;
    }

    case NodeDataChanged: {
      for (ZkListener listener : this.listeners) {
        listener.nodeDataChanged(event.getPath());
      }
      break;
    }

    case NodeChildrenChanged: {
      for (ZkListener listener : this.listeners) {
        listener.nodeChildrenChanged(event.getPath());
      }
      break;
    }
    }
  }

  public byte[] getData(String path, Watcher watcher, Stat stat)
      throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        // 120227 by DaeJin Choi - What about Metadata of data ( ex: Magic
        // number and so on )
        return zk.getData(path, watcher, stat);
      } catch (KeeperException e) {
        switch (e.code()) {
        case CONNECTIONLOSS:
        case OPERATIONTIMEOUT:
          LOG.warn("Possibly transient ZooKeeper exception: " + e);
          if (!retryCounter.shouldRetry()) {
            LOG.error("ZooKeeper getData failed after "
                + retryCounter.getMaxRetries() + " retries");
            throw e;
          }
          break;

        default:
          throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  public List<String> getChildren(String path) throws KeeperException,
      InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.getChildren(path, true);
      } catch (KeeperException e) {
        switch (e.code()) {
        case CONNECTIONLOSS:
        case OPERATIONTIMEOUT:
          LOG.warn("Possibly transient ZooKeeper exception: " + e);
          if (!retryCounter.shouldRetry()) {
            LOG.error("ZooKeeper getChildren failed after "
                + retryCounter.getMaxRetries() + " retries");
            throw e;
          }
          break;

        default:
          throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  public ZooKeeper getClient() {
    return this.zk;
  }

  public void close() {
    try {
      zk.close();
    } catch (InterruptedException e) {
      LOG.error(e);
    }
  }
}
