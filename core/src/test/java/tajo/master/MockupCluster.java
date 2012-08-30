package tajo.master;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import tajo.TajoTestingUtility;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.master.TajoMaster;

import java.util.List;

/**
 * @author Jihoon Son
 */
public class MockupCluster {
  static final Log LOG = LogFactory.getLog(MockupCluster.class);

  private TajoMaster master;
  private List<MockupWorker> workers;
  private final TajoConf conf;
  private final int numWorkers;
  private final int numNormalWorkers;
  private final int numAbortWorkers;
  private final int numShutdownWorkers;
  private final TajoTestingUtility util;

  public MockupCluster(int numWorkers) throws Exception {
    this(numWorkers, 0, 0);
  }

  public MockupCluster(int numWorkers,
                       int numAbortWorkers, int numShutdownWorkers)
      throws Exception {
    this.util = new TajoTestingUtility();
    this.util.startMiniDFSCluster(numWorkers);
    this.conf = util.getConfiguration();
    conf.setVar(ConfVars.ENGINE_BASE_DIR,
        util.getMiniDFSCluster().getFileSystem().getUri()+"/tajo");
    conf.setVar(ConfVars.MASTER_ADDRESS, "localhost:0");
    conf.setVar(ConfVars.CATALOG_ADDRESS, "localhost:0");
    conf.setIntVar(ConfVars.LEAFSERVER_PORT, 0);

    this.numWorkers = numWorkers;
    this.numAbortWorkers = numAbortWorkers;
    this.numShutdownWorkers = numShutdownWorkers;
    this.numNormalWorkers = numWorkers - (numAbortWorkers + numShutdownWorkers);
    this.master = new TajoMaster(conf);

    this.workers = Lists.newArrayList();

    int i;
    for (i = 0; i < numNormalWorkers; i++) {
      workers.add(new MockupNormalWorker(conf));
    }
    for (i = 0; i < numAbortWorkers; i++) {
      workers.add(new MockupAbortWorker(conf));
    }
    for (i = 0; i < numShutdownWorkers; i++) {
      workers.add(new MockupShutdownWorker(conf, 10000));
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  public TajoMaster getMaster() {
    return this.master;
  }

  public List<MockupWorker> getWorkers() {
    return this.workers;
  }

  public MockupWorker getWorker(int index) {
    return this.workers.get(index);
  }

  public int getClusterSize() {
    return workers.size();
  }

  public void start() {
    master.start();
    while (true) {
      if (master.isMasterRunning()) {
        break;
      }
    }

    for (MockupWorker worker : workers) {
      worker.start();
    }
  }

  public void shutdown() throws Exception {
    if (this.workers != null) {
      for(MockupWorker t: this.workers) {
        if (t.isAlive()) {
          try {
            t.shutdown("Shutdown");
            t.join();
          } catch (InterruptedException e) {
            // continue
          }
        }
      }
    }
    master.shutdown();
    this.util.shutdownMiniDFSCluster();
  }

  public void join() {
    if (this.workers != null) {
      for(Thread t: this.workers) {
        if (t.isAlive()) {
          try {
            t.join();
          } catch (InterruptedException e) {
            // continue
          }
        }
      }
    }

    if(master.isAlive()) {
      try {
        master.join();
      } catch (InterruptedException e) {
        // continue
      }
    }
  }
}
