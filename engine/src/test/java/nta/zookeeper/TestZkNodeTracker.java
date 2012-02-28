package nta.zookeeper;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Semaphore;

import nta.engine.Abortable;
import nta.engine.NConstants;
import nta.engine.NtaTestingUtility;
import nta.engine.utils.ThreadUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestZkNodeTracker {
  private final static Log LOG = LogFactory.getLog(TestZkNodeTracker.class);

  private final static NtaTestingUtility TEST_UTIL = new NtaTestingUtility();
  private static Configuration conf;

  private final static Random rand = new Random();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    conf = TEST_UTIL.getConfiguration();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testInterruptible() throws IOException, InterruptedException {
    ZkClient zkClient = new ZkClient(conf);
    final TestTracker tracker = new TestTracker(zkClient, "/xyz");
    tracker.start();
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          tracker.blockUntilAvailable();
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted", e);
        }
      }
    };
    t.start();
    while (!t.isAlive())
      ThreadUtil.sleep(1);
    tracker.stop();
    t.join();
    // If it wasn't interruptible, we'd never get to here.
  }

  @Test
  public void testNodeTracker() throws Exception {
    ZkClient zk = new ZkClient(TEST_UTIL.getConfiguration());
    ZkUtil.createAndFailSilent(zk, "/test");

    final String node =
        ZkUtil.concat("/test", new Long(rand.nextLong()).toString());

    final byte[] dataOne = "dataOne".getBytes();
    final byte[] dataTwo = "dataTwo".getBytes();

    // Start a ZKNT with no node currently available
    TestTracker localTracker = new TestTracker(zk, node);
    localTracker.start();
    zk.subscribe(localTracker);

    // Make sure we don't have a node
    assertNull(localTracker.getData());

    // Spin up a thread with another ZKNT and have it block
    WaitToGetDataThread thread = new WaitToGetDataThread(zk, node);
    thread.start();

    // Verify the thread doesn't have a node
    assertFalse(thread.hasData);

    // Now, start a new ZKNT with the node already available
    TestTracker secondTracker = new TestTracker(zk, node);
    secondTracker.start();
    zk.subscribe(secondTracker);

    // Put up an additional zk listener so we know when zk event is done
    TestingZKListener zkListener = new TestingZKListener(zk, node);
    zk.subscribe(zkListener);
    assertEquals(0, zkListener.createdLock.availablePermits());

    // Create a completely separate zk connection for test triggers and avoid
    // any weird watcher interactions from the test
    final ZooKeeper zkconn =
        new ZooKeeper(conf.get(NConstants.ZOOKEEPER_ADDRESS),
            NConstants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT, new StubWatcher());

    // Add the node with data one
    zkconn.create(node, dataOne, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    // Wait for the zk event to be processed
    zkListener.waitForCreation();
    thread.join();

    // Both trackers should have the node available with data one
    assertNotNull(localTracker.getData());
    assertNotNull(localTracker.blockUntilAvailable());
    assertEquals(new String(dataOne), new String(localTracker.getData()));
    assertTrue(thread.hasData);
    assertEquals(new String(dataOne), new String(thread.tracker.getData()));
    LOG.info("Successfully got data one");

    // Make sure it's available and with the expected data
    assertNotNull(secondTracker.getData());
    assertNotNull(secondTracker.blockUntilAvailable());
    assertEquals(new String(dataOne), new String(secondTracker.getData()));
    LOG.info("Successfully got data one with the second tracker");

    // Drop the node
    zkconn.delete(node, -1);
    zkListener.waitForDeletion();

    // Create a new thread but with the existing thread's tracker to wait
    TestTracker threadTracker = thread.tracker;
    thread = new WaitToGetDataThread(zk, node, threadTracker);
    thread.start();

    // Verify other guys don't have data
    assertFalse(thread.hasData);
    assertNull(secondTracker.getData());
    assertNull(localTracker.getData());
    LOG.info("Successfully made unavailable");

    // Create with second data
    zkconn.create(node, dataTwo, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    // Wait for the zk event to be processed
    zkListener.waitForCreation();
    thread.join();

    // All trackers should have the node available with data two
    assertNotNull(localTracker.getData());
    assertNotNull(localTracker.blockUntilAvailable());
    assertEquals(new String(localTracker.getData()), new String(dataTwo));
    assertNotNull(secondTracker.getData());
    assertNotNull(secondTracker.blockUntilAvailable());
    assertEquals(new String(secondTracker.getData()), new String(dataTwo));
    assertTrue(thread.hasData);
    assertEquals(new String(thread.tracker.getData()), new String(dataTwo));
    LOG.info("Successfully got data two on all trackers and threads");

    // Change the data back to data one
    zkconn.setData(node, dataOne, -1);

    // Wait for zk event to be processed
    zkListener.waitForDataChange();

    // All trackers should have the node available with data one
    assertNotNull(localTracker.getData());
    assertNotNull(localTracker.blockUntilAvailable());
    assertEquals(new String(localTracker.getData()), new String(dataOne));
    assertNotNull(secondTracker.getData());
    assertNotNull(secondTracker.blockUntilAvailable());
    assertEquals(new String(secondTracker.getData()), new String(dataOne));
    assertTrue(thread.hasData);
    assertEquals(new String(thread.tracker.getData()), new String(dataOne));
    LOG.info("Successfully got data one following a data change on all trackers and threads");
  }

  public static class WaitToGetDataThread extends Thread {

    TestTracker tracker;
    boolean hasData;

    public WaitToGetDataThread(ZkClient zk, String node) {
      tracker = new TestTracker(zk, node);
      tracker.start();
      zk.subscribe(tracker);
      hasData = false;
    }

    public WaitToGetDataThread(ZkClient zk, String node, TestTracker tracker) {
      this.tracker = tracker;
      hasData = false;
    }

    @Override
    public void run() {
      LOG.info("Waiting for data to be available in WaitToGetDataThread");
      try {
        tracker.blockUntilAvailable();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      LOG.info("Data now available in tracker from WaitToGetDataThread");
      hasData = true;
    }
  }

  public static class TestTracker extends ZkNodeTracker {
    public TestTracker(ZkClient client, String node) {
      super(client, node);
    }
  }

  public static class TestingZKListener extends ZkListener {
    private static final Log LOG = LogFactory.getLog(TestingZKListener.class);

    private Semaphore deletedLock;
    private Semaphore createdLock;
    private Semaphore changedLock;
    private String node;

    public TestingZKListener(ZkClient watcher, String node) {
      deletedLock = new Semaphore(0);
      createdLock = new Semaphore(0);
      changedLock = new Semaphore(0);
      this.node = node;
    }

    @Override
    public void nodeDeleted(String path) {
      if (path.equals(node)) {
        LOG.debug("nodeDeleted(" + path + ")");
        deletedLock.release();
      }
    }

    @Override
    public void nodeCreated(String path) {
      if (path.equals(node)) {
        LOG.debug("nodeCreated(" + path + ")");
        createdLock.release();
      }
    }

    @Override
    public void nodeDataChanged(String path) {
      if (path.equals(node)) {
        LOG.debug("nodeDataChanged(" + path + ")");
        changedLock.release();
      }
    }

    public void waitForDeletion() throws InterruptedException {
      deletedLock.acquire();
    }

    public void waitForCreation() throws InterruptedException {
      createdLock.acquire();
    }

    public void waitForDataChange() throws InterruptedException {
      changedLock.acquire();
    }
  }

  public static class StubWatcher implements Watcher {
    public void process(WatchedEvent event) {
    }
  }

  public static class StubAbortable implements Abortable {
    @Override
    public void abort(final String msg, final Throwable t) {
    }
  }
}
